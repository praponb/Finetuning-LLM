from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_city_by_capacity = Window.partitionBy("city").orderBy("capacity", "id")
w_opened_on_year = Window.partitionBy(F.year("opened_on"))
w_all_by_capacity_ties = Window.orderBy("capacity")
w_city_by_capacity_running = Window.partitionBy("city").orderBy("capacity", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_capacity_prior_1 = Window.orderBy("capacity", "id").rowsBetween(-1, -1)

filtered = (
    df_warehouses
    .filter(F.upper(F.trim(F.col("wh_code"))).like("B%"))
    .select(
        "id",
        "city",
        "capacity",
        "opened_on",
        F.concat_ws(" / ", F.col("city"), F.col("wh_code")).alias("city_wh_code_label"),
    )
)

with_trends = (
    filtered
    .select(
        "id",
        "city",
        "capacity",
        "opened_on",
        "city_wh_code_label",
        F.ntile(5).over(w_city_by_capacity).alias("capacity_quintile"),
        F.size(F.collect_set("city").over(w_opened_on_year)).alias("distinct_city_in_opened_on_year"),
        F.rank().over(w_all_by_capacity_ties).alias("capacity_rank"),
        F.min("capacity").over(w_city_by_capacity_running).alias("min_capacity_so_far"),
        F.max("capacity").over(w_all_by_capacity_prior_1).alias("capacity_max_1_prior"),
    )
)

labeled = (
    with_trends
    .filter(F.col("min_capacity_so_far") <= 130350)
    .select(
        "*",
        F.date_add(F.col("opened_on"), 30).alias("opened_on_plus_30d"),
        (F.floor(F.col("min_capacity_so_far") / 50000) * 50000).alias("min_capacity_so_far_bucket"),
    )
)

top_rows = (
    labeled
    .filter((F.col("capacity_rank") == 1) & (F.col("opened_on_plus_30d") >= F.lit("2024-05-01").cast("date")))
    .drop("capacity_rank")
)

city_stats = (
    top_rows
    .groupBy("city")
    .agg(F.max("min_capacity_so_far").alias("max_min_capacity_so_far"))
)

outliers = (
    top_rows.alias("s")
    .join(city_stats.alias("g"), F.col("g.city") == F.col("s.city"), "inner")
    .filter(F.col("s.min_capacity_so_far") == F.col("g.max_min_capacity_so_far"))
    .select(
        "s.*",
        "g.max_min_capacity_so_far",
        F.round(F.col("s.min_capacity_so_far") - F.col("g.max_min_capacity_so_far"), 2).alias("min_capacity_so_far_minus_max"),
    )
)

result = (
    outliers
    .select(
        "id",
        "city",
        "capacity",
        "distinct_city_in_opened_on_year",
        "min_capacity_so_far",
        "capacity_max_1_prior",
        "min_capacity_so_far_bucket",
        "min_capacity_so_far_minus_max",
        F.date_format(F.col("opened_on_plus_30d"), "MMM yyyy").alias("opened_on_plus_30d_label"),
    )
    .orderBy("distinct_city_in_opened_on_year", "id")
    .limit(20)
)
