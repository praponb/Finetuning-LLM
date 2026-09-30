from pyspark.sql import functions as F

scoped_warehouses = df_warehouses.filter(F.col("wh_code").isNotNull())

move_stats = (
    df_stock_moves
    .groupBy("warehouse_id")
    .agg(F.count("*").alias("move_count"))
)

with_stats = (
    scoped_warehouses.alias("s")
    .join(move_stats.alias("st"), F.col("st.warehouse_id") == F.col("s.id"), "left")
    .select("s.*", F.coalesce(F.col("st.move_count"), F.lit(0)).alias("move_count"))
)

qualified = (
    with_stats.alias("s")
    .join(df_stock_moves.alias("st"), (F.col("st.warehouse_id") == F.col("s.id")) & (F.col("st.unit_price") <= 292.3) & (F.col("st.qty") < 256), "left_anti")
    .filter(F.col("city") == "Bangkok")
)

city_stats = (
    qualified
    .groupBy("city")
    .agg(F.max("move_count").alias("max_move_count"))
)

outliers = (
    qualified.alias("s")
    .join(city_stats.alias("g"), F.col("g.city") == F.col("s.city"), "inner")
    .filter(F.col("s.move_count") == F.col("g.max_move_count"))
    .select(
        "s.*",
        "g.max_move_count",
        F.round(F.col("s.move_count") - F.col("g.max_move_count"), 2).alias("move_count_minus_max"),
    )
)

result = (
    outliers
    .groupBy("move_count")
    .pivot("city", ["Singapore", "Bangkok"])
    .agg(F.min("capacity"))
)
