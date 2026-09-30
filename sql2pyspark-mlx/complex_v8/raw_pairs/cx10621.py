from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_city_by_max_capacity = Window.partitionBy("city").orderBy("max_capacity", "id")
w_opened_on_week_month_city = Window.partitionBy(F.date_trunc("month", "opened_on_week"), "city")

base_warehouses = (
    df_warehouses
    .filter((F.col("city") == "Kuala Lumpur") | (F.col("capacity") > 11005))
    .select(
        "id",
        "wh_code",
        "city",
        "capacity",
        F.date_trunc("week", F.col("opened_on")).alias("opened_on_week"),
        (F.floor(F.col("capacity") / 5000) * 5000).alias("capacity_bucket"),
        F.lower(F.trim(F.col("city"))).alias("city_clean"),
    )
)

city_stats = (
    base_warehouses
    .groupBy("city")
    .agg(F.max("capacity").alias("max_capacity"))
)

outliers = (
    base_warehouses.alias("s")
    .join(city_stats.alias("g"), F.col("g.city") == F.col("s.city"), "inner")
    .filter(F.col("s.capacity") == F.col("g.max_capacity"))
    .select(
        "s.*",
        "g.max_capacity",
        F.round(F.col("s.capacity") - F.col("g.max_capacity"), 2).alias("capacity_minus_max"),
    )
)

windowed = (
    outliers
    .select(
        "id",
        "wh_code",
        "city",
        "opened_on_week",
        "capacity_bucket",
        "max_capacity",
        "capacity_minus_max",
        F.round(F.percent_rank().over(w_city_by_max_capacity), 3).alias("max_capacity_percent_rank"),
        (F.col("capacity") - F.lag("capacity").over(w_city_by_max_capacity)).alias("capacity_change"),
        F.round(F.col("capacity_bucket") / F.nullif(F.sum("capacity_bucket").over(w_opened_on_week_month_city), F.lit(0)), 4).alias("capacity_bucket_share_of_opened_on_week_month_ci"),
    )
)

result = (
    windowed
    .select(
        "id",
        "wh_code",
        "opened_on_week",
        "capacity_bucket",
        "capacity_minus_max",
        "max_capacity_percent_rank",
        "capacity_change",
        F.coalesce(F.col("city"), F.lit("UNKNOWN")).alias("city_or_unknown"),
        F.least(F.col("capacity_bucket"), F.col("capacity_minus_max")).alias("least_capacity_bucket_capacity_minus_max"),
    )
    .orderBy(F.col("opened_on_week").asc())
)
