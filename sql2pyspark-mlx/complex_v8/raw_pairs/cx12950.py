from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_capacity_tier = Window.partitionBy("capacity_tier")
w_all_by_days_since_opened_on = Window.orderBy(F.col("days_since_opened_on").desc(), "id")
w_all_by_days_since_opened_on_2 = Window.orderBy(F.col("days_since_opened_on").desc(), "id").rowsBetween(-1, Window.currentRow)
w_n_city_singapore_by_distinct = Window.partitionBy("n_city_singapore").orderBy(F.col("distinct_citys").asc_nulls_last(), "city", "prev_opened_on_year")
w_prev_opened_on_year = Window.partitionBy("prev_opened_on_year")

prepared = (
    df_warehouses
    .filter((F.col("capacity") > 11005) & (F.upper(F.trim(F.col("wh_code"))).like("C%")))
    .select(
        "id",
        "wh_code",
        "city",
        "opened_on",
        F.datediff(F.lit("2025-06-30").cast("date"), F.col("opened_on")).alias("days_since_opened_on"),
        F.when(F.col("capacity") >= 11506, "large").when(F.col("capacity") >= 4912, "medium").otherwise("small").alias("capacity_tier"),
    )
)

metrics = (
    prepared
    .select(
        "id",
        "city",
        "opened_on",
        "days_since_opened_on",
        F.max("days_since_opened_on").over(w_capacity_tier).alias("capacity_tier_max_days_since_opened_on"),
        F.lag("opened_on").over(w_all_by_days_since_opened_on).alias("prev_opened_on"),
        F.sum("days_since_opened_on").over(w_all_by_days_since_opened_on_2).alias("days_since_opened_on_sum_2"),
        F.datediff(F.col("opened_on"), F.lag("opened_on").over(w_all_by_days_since_opened_on)).alias("days_since_prev_opened_on"),
        F.round(F.avg("days_since_opened_on").over(w_all_by_days_since_opened_on_2), 2).alias("days_since_opened_on_avg_2"),
    )
)

totals = (
    metrics
    .groupBy("city", F.year("prev_opened_on").alias("prev_opened_on_year"))
    .agg(
        F.sum(F.when(F.col("city") == "Singapore", 1).otherwise(0)).alias("n_city_singapore"),
        F.count("*").alias("n_rows"),
        F.countDistinct("city").alias("distinct_citys"),
        F.array_sort(F.collect_set("city")).alias("city_list"),
        F.datediff(F.max("opened_on"), F.min("opened_on")).alias("opened_on_span_days"),
    )
)

ranked = (
    totals
    .select(
        "city",
        "prev_opened_on_year",
        "city_list",
        "opened_on_span_days",
        F.ntile(3).over(w_n_city_singapore_by_distinct).alias("distinct_citys_tercile"),
        F.size(F.collect_set("city").over(w_prev_opened_on_year)).alias("distinct_city_in_prev_opened_on_year"),
        F.round(F.col("n_city_singapore") / F.nullif(F.sum("n_city_singapore").over(w_prev_opened_on_year), F.lit(0)), 4).alias("n_city_singapore_share_of_prev_opened_on_year"),
        F.round(F.col("prev_opened_on_year") / F.nullif(F.sum("prev_opened_on_year").over(w_prev_opened_on_year), F.lit(0)), 4).alias("prev_opened_on_year_share_of_prev_opened_on_year"),
        F.min("n_city_singapore").over(w_prev_opened_on_year).alias("prev_opened_on_year_min_n_city_singapore"),
        F.count(F.lit(1)).over(w_prev_opened_on_year).alias("rows_in_prev_opened_on_year"),
    )
)

annotated = (
    ranked
    .select(
        "*",
        F.concat(F.col("city"), F.lit("#"), F.col("prev_opened_on_year").cast("string")).alias("city_ref"),
        F.array_join(F.array_sort(F.col("city_list")), ",").alias("city_list_text"),
        F.substring(F.trim(F.col("city")), 1, 2).alias("city_prefix"),
    )
)

result = (
    annotated
    .select(
        "city",
        "prev_opened_on_year",
        "city_list",
        "opened_on_span_days",
        "distinct_citys_tercile",
        "distinct_city_in_prev_opened_on_year",
        "prev_opened_on_year_share_of_prev_opened_on_year",
        "prev_opened_on_year_min_n_city_singapore",
        "rows_in_prev_opened_on_year",
        "city_ref",
    )
    .orderBy("opened_on_span_days", F.col("city_ref").desc())
)
