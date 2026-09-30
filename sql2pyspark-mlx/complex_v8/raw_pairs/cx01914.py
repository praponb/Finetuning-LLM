from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_latest = Window.partitionBy("move_operator").orderBy(F.col("move_count").desc(), F.col("id").desc(), F.col("move_id").desc())

cleaned_warehouses = (
    df_warehouses
    .filter(F.col("city") == "Bangkok")
    .select(
        "id",
        "wh_code",
        "city",
        "capacity",
        F.coalesce(F.col("capacity"), F.lit(0)).alias("capacity_filled"),
    )
)

without_matches = (
    cleaned_warehouses.alias("s")
    .join(df_stock_moves.alias("st"), (F.col("st.warehouse_id") == F.col("s.id")) & (~F.col("st.move_type").isin("ADJUST", "TRANSFER")), "left_anti")
)

move_stats = (
    df_stock_moves
    .groupBy("warehouse_id")
    .agg(F.count("*").alias("move_count"), F.count("*").alias("move_count_2"))
)

with_counts = (
    without_matches.alias("s")
    .join(move_stats.alias("st"), F.col("st.warehouse_id") == F.col("s.id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("st.move_count"), F.lit(0)).alias("move_count"),
        F.coalesce(F.col("st.move_count_2"), F.lit(0)).alias("move_count_2"),
    )
)

line_level_moves = (
    with_counts.alias("s")
    .join(df_stock_moves.alias("st"), F.col("st.warehouse_id") == F.col("s.id"), "inner")
    .select(
        "s.id",
        "s.wh_code",
        "s.city",
        "s.capacity_filled",
        "s.move_count",
        F.col("st.id").alias("move_id"),
        F.col("st.unit_price").alias("move_unit_price"),
        F.col("st.operator").alias("move_operator"),
        F.col("st.sku_id").alias("move_sku_id"),
        F.col("st.qty").alias("move_qty"),
    )
)

deduped = (
    line_level_moves
    .withColumn("recency_rank", F.row_number().over(w_latest))
    .filter(F.col("recency_rank") == 1)
    .drop("recency_rank")
)

move_operator_stats = (
    deduped
    .groupBy("move_operator")
    .agg(F.max("move_unit_price").alias("max_move_unit_price"))
)

outliers = (
    deduped.alias("s")
    .join(move_operator_stats.alias("g"), F.col("g.move_operator") == F.col("s.move_operator"), "inner")
    .filter(F.col("s.move_unit_price") == F.col("g.max_move_unit_price"))
    .select(
        "s.*",
        "g.max_move_unit_price",
        F.round(F.col("s.move_unit_price") - F.col("g.max_move_unit_price"), 2).alias("move_unit_price_minus_max"),
    )
)

result = (
    outliers
    .select(
        "id",
        "wh_code",
        "city",
        "capacity_filled",
        "move_count",
        "move_unit_price",
        "move_operator",
        "move_sku_id",
        "move_qty",
        "max_move_unit_price",
        "move_unit_price_minus_max",
        F.lower(F.trim(F.col("wh_code"))).alias("wh_code_clean"),
        F.coalesce(F.col("move_qty"), F.lit(0)).alias("move_qty_filled"),
    )
    .orderBy(F.col("id").desc())
)
