from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_city = Window.partitionBy("city")
w_city_by_capacity = Window.partitionBy("city").orderBy(F.col("capacity").asc_nulls_last(), "id")

valid = (
    df_warehouses
    .filter(F.lower(F.col("wh_code")).like("%l1%"))
    .select(
        "id",
        "wh_code",
        "city",
        "capacity",
        (F.floor(F.col("capacity") / 5000) * 5000).alias("capacity_bucket"),
    )
)

metrics = (
    valid
    .select(
        "*",
        F.round(F.col("capacity") - F.avg("capacity").over(w_city), 2).alias("capacity_vs_city_avg"),
        F.lag("capacity_bucket").over(w_city_by_capacity).alias("prev_capacity_bucket"),
        F.ntile(10).over(w_city_by_capacity).alias("capacity_decile"),
    )
)

labeled = (
    metrics
    .select(
        "id",
        "capacity",
        "capacity_bucket",
        "capacity_vs_city_avg",
        "prev_capacity_bucket",
        "capacity_decile",
        F.abs(F.col("prev_capacity_bucket") - 9550).alias("prev_capacity_bucket_distance"),
    )
)

result = (
    labeled
    .select(
        "id",
        "capacity",
        "capacity_vs_city_avg",
        "capacity_decile",
        "prev_capacity_bucket_distance",
        (F.col("capacity") - F.col("capacity_bucket")).alias("capacity_minus_capacity_bucket"),
        F.round(F.col("capacity") / F.nullif(F.col("capacity_bucket"), F.lit(0)), 3).alias("capacity_per_capacity_bucket"),
    )
    .orderBy(F.col("id").desc_nulls_first(), "prev_capacity_bucket_distance")
    .limit(20)
)
