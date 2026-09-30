from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_latest = Window.partitionBy("move_type").orderBy(F.col("unit_price").desc(), F.col("id").desc())

filtered_moves = (
    df_stock_moves
    .filter((F.col("moved_at") < F.lit("2025-03-02 10:00:00").cast("timestamp")) & (F.col("move_type") == "ADJUST"))
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "moved_at",
        "unit_price",
        F.month(F.col("moved_at")).alias("moved_at_month"),
        F.round(F.col("unit_price") / F.nullif(F.col("qty"), F.lit(0)), 3).alias("unit_price_per_qty"),
    )
)

deduped = (
    filtered_moves
    .withColumn("rn", F.row_number().over(w_latest))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

move_stats = (
    df_stock_moves
    .groupBy("warehouse_id")
    .agg(
        F.min("moved_at").alias("first_moved_at"),
        F.max("moved_at").alias("last_moved_at"),
        F.max("moved_at").alias("last_moved_at_2"),
    )
)

with_stats = (
    deduped.alias("s")
    .join(move_stats.alias("st"), F.col("st.warehouse_id") == F.col("s.warehouse_id"), "left")
    .select(
        "s.*",
        "st.first_moved_at",
        "st.last_moved_at",
        "st.last_moved_at_2",
    )
)

qualified = (
    with_stats.alias("s")
    .join(df_stock_moves.alias("st"), (F.col("st.sku_id") == F.col("s.sku_id")) & (F.col("st.operator") == "op_chan"), "left_anti")
    .filter(F.col("last_moved_at_2") >= F.lit("2025-01-25 09:00:00").cast("timestamp"))
)

result = (
    qualified
    .rollup("warehouse_id", "move_type")
    .agg(
        F.grouping("warehouse_id").alias("is_total_warehouse_id"),
        F.count(F.when(~F.col("move_type").isin("OUTBOUND", "INBOUND"), 1)).alias("n_move_type_not_in_outbound_in"),
    )
)
