from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_dedup = Window.partitionBy("operator", "sku_category").orderBy(F.col("qty").desc(), F.col("id").desc())

valid = (
    df_stock_moves
    .filter((F.col("unit_price") < 298.9) & (F.col("move_type").isin("ADJUST", "INBOUND", "TRANSFER")))
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "qty",
        "moved_at",
        "operator",
        "unit_price",
        F.coalesce(F.col("qty"), F.lit(0)).alias("qty_filled"),
        F.greatest(F.col("qty"), F.col("unit_price")).alias("greatest_qty_unit_price"),
    )
)

move_stats = (
    df_stock_moves
    .groupBy("sku_id")
    .agg(F.count("*").alias("move_count"))
)

with_stats = (
    valid.alias("s")
    .join(move_stats.alias("st"), F.col("st.sku_id") == F.col("s.sku_id"), "left")
    .select("s.*", F.coalesce(F.col("st.move_count"), F.lit(0)).alias("move_count"))
)

combined_details = (
    with_stats.alias("s")
    .join(df_warehouses.alias("w"), F.col("w.id") == F.col("s.warehouse_id"), "inner")
    .join(df_skus.alias("sk"), F.col("sk.id") == F.col("s.sku_id"), "left")
    .filter(F.col("w.wh_code").isNotNull())
    .select(
        "s.id",
        "s.warehouse_id",
        "s.sku_id",
        "s.qty",
        "s.operator",
        "s.unit_price",
        "s.qty_filled",
        F.col("w.wh_code").alias("warehouse_wh_code"),
        F.col("sk.category").alias("sku_category"),
        F.col("sk.labels").alias("sku_labels"),
    )
)

deduped = (
    combined_details
    .withColumn("rn", F.row_number().over(w_dedup))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

move_stats_2 = (
    df_stock_moves
    .groupBy("sku_id")
    .agg(
        F.count("*").alias("move_count"),
        F.count(F.when(F.col("operator") != "op_dew", 1)).alias("move_operator_count"),
        F.count("*").alias("move_count_2"),
    )
)

with_counts = (
    deduped.alias("s")
    .join(move_stats_2.alias("st"), F.col("st.sku_id") == F.col("s.sku_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("st.move_count"), F.lit(0)).alias("move_count"),
        F.coalesce(F.col("st.move_operator_count"), F.lit(0)).alias("move_operator_count"),
        F.coalesce(F.col("st.move_count_2"), F.lit(0)).alias("move_count_2"),
    )
)

result = (
    with_counts
    .select(
        "id",
        "sku_id",
        "operator",
        "unit_price",
        "qty_filled",
        "sku_labels",
        "move_count",
        F.greatest(F.col("unit_price"), F.col("move_count")).alias("greatest_unit_price_move_count"),
    )
    .orderBy("id")
    .limit(20)
)
