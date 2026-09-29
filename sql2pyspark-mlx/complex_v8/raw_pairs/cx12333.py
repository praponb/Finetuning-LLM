from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_opened_on_month = Window.partitionBy(F.month("opened_on"))
w_city_or_unknown_by_capacity = Window.partitionBy("city_or_unknown").orderBy("capacity_vs_opened_on_month_avg", "id")
w_distinct_city_in_opened_on_m = Window.partitionBy("distinct_city_in_opened_on_month", "city_or_unknown")
w_city_or_unknown_by_capacity_2 = Window.partitionBy("city_or_unknown").orderBy("capacity_vs_opened_on_month_avg", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

prepared = (
    df_warehouses
    .filter(F.upper(F.trim(F.col("wh_code"))).like("K%"))
    .select(
        "id",
        "wh_code",
        "city",
        "capacity",
        "opened_on",
        F.coalesce(F.col("city"), F.lit("UNKNOWN")).alias("city_or_unknown"),
    )
)

with_trends = (
    prepared
    .select(
        "*",
        F.round(F.col("capacity") - F.avg("capacity").over(w_opened_on_month), 2).alias("capacity_vs_opened_on_month_avg"),
        F.size(F.collect_set("city").over(w_opened_on_month)).alias("distinct_city_in_opened_on_month"),
    )
)

sequenced = (
    with_trends
    .select(
        "id",
        "wh_code",
        "city",
        "opened_on",
        "capacity_vs_opened_on_month_avg",
        "distinct_city_in_opened_on_month",
        F.lag("capacity").over(w_city_or_unknown_by_capacity).alias("prev_capacity"),
        F.max("capacity").over(w_distinct_city_in_opened_on_m).alias("distinct_city_in_opened_on_month_city_or_unknown"),
        F.size(F.collect_set("city").over(w_distinct_city_in_opened_on_m)).alias("distinct_city_in_distinct_city_in_opened_on_mont"),
        F.round(F.avg("capacity").over(w_distinct_city_in_opened_on_m), 2).alias("distinct_city_in_opened_on_month_city_or_unknown_2"),
        F.nth_value("capacity", 2).over(w_city_or_unknown_by_capacity_2).alias("second_capacity"),
        F.last("capacity").over(w_city_or_unknown_by_capacity_2).alias("last_capacity"),
    )
)

grouped = (
    sequenced
    .groupBy(
        "city",
        F.date_format("opened_on", "yyyy-MM").alias("opened_on_ym"),
        "distinct_city_in_opened_on_month",
    )
    .agg(
        F.array_sort(F.collect_set("city")).alias("city_list"),
        F.count("*").alias("cnt"),
        F.count(F.when(F.lower(F.col("wh_code")).like("%nx%"), 1)).alias("n_lower_wh_code_like_nx"),
        F.countDistinct("id").alias("distinct_ids"),
        F.sum("prev_capacity").alias("total_prev_capacity"),
    )
)

result = (
    grouped
    .select(
        "city",
        "opened_on_ym",
        "total_prev_capacity",
        "distinct_ids",
    )
    .unpivot(
        ids=["city", "opened_on_ym"],
        values=["total_prev_capacity", "distinct_ids"],
        variableColumnName="measure",
        valueColumnName="reading",
    )
    .filter(F.col("reading").isNotNull())
    .orderBy("city", "opened_on_ym", "measure")
)
