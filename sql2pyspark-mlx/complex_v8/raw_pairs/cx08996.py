from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_latest = Window.partitionBy("city").orderBy(F.col("opened_on").desc(), F.col("id").desc())

cleaned_warehouses = (
    df_warehouses
    .filter(F.col("opened_on") < F.lit("2025-02-14").cast("date"))
    .select(
        "id",
        "wh_code",
        "city",
        "opened_on",
        F.abs(F.col("capacity") - 4912).alias("capacity_distance"),
        F.abs(F.col("capacity") - 11005).alias("capacity_distance_2"),
        F.when(F.col("capacity") >= 11506, "A").when(F.col("capacity") >= 4912, "B").otherwise("C").alias("capacity_tier"),
    )
)

move_stats = (
    df_stock_moves
    .groupBy("warehouse_id")
    .agg(F.max("moved_at").alias("last_moved_at"))
)

summarized_rows = (
    cleaned_warehouses.alias("s")
    .join(move_stats.alias("st"), F.col("st.warehouse_id") == F.col("s.id"), "left")
    .select("s.*", "st.last_moved_at")
)

deduped = (
    summarized_rows
    .withColumn("rn", F.row_number().over(w_latest))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

capacity_tier_stats = (
    deduped
    .groupBy("capacity_tier")
    .agg(F.max("capacity_distance").alias("max_capacity_distance"))
)

above_average = (
    deduped.alias("s")
    .join(capacity_tier_stats.alias("g"), F.col("g.capacity_tier") == F.col("s.capacity_tier"), "inner")
    .filter(F.col("s.capacity_distance") == F.col("g.max_capacity_distance"))
    .select(
        "s.*",
        "g.max_capacity_distance",
        F.round(F.col("s.capacity_distance") - F.col("g.max_capacity_distance"), 2).alias("capacity_distance_minus_max"),
    )
)

city_stats = (
    above_average
    .groupBy("city")
    .agg(F.percentile_approx("capacity_distance", 0.5).alias("median_capacity_distance"))
)

outliers = (
    above_average.alias("s")
    .join(city_stats.alias("g"), F.col("g.city") == F.col("s.city"), "inner")
    .filter(F.col("s.capacity_distance") >= F.col("g.median_capacity_distance"))
    .select(
        "s.*",
        "g.median_capacity_distance",
        F.round(F.col("s.capacity_distance") - F.col("g.median_capacity_distance"), 2).alias("capacity_distance_minus_median"),
    )
)

filtered = (
    outliers.alias("s")
    .join(df_stock_moves.alias("st"), (F.col("st.warehouse_id") == F.col("s.id")) & (F.col("st.sku_id") <= 9), "left_anti")
    .filter(F.col("capacity_distance") <= 17000)
)

result = (
    filtered
    .filter(F.col("capacity_tier").isin("B", "C"))
    .select(
        "id",
        "wh_code",
        "city",
        "opened_on",
        "capacity_distance",
        "capacity_distance_2",
        "capacity_tier",
        "last_moved_at",
        "max_capacity_distance",
        "capacity_distance_minus_max",
        "median_capacity_distance",
        F.substring(F.trim(F.col("capacity_tier")), 1, 3).alias("capacity_tier_prefix"),
    )
    .orderBy(F.col("capacity_distance_2").desc_nulls_first(), "opened_on")
)
