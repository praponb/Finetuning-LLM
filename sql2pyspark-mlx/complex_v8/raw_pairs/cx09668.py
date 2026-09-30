from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_dedup = Window.partitionBy("operator").orderBy("qty", "id")

base = (
    df_stock_moves
    .filter(F.hour(F.col("moved_at")).between(6, 11))
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "moved_at",
        "operator",
        F.to_date(F.col("moved_at")).alias("moved_at_date"),
    )
)

with_details = (
    base.alias("s")
    .join(df_warehouses.alias("w"), F.col("w.id") == F.col("s.warehouse_id"), "inner")
    .select(
        "s.id",
        "s.warehouse_id",
        "s.sku_id",
        "s.move_type",
        "s.qty",
        "s.operator",
        F.col("w.wh_code").alias("warehouse_wh_code"),
        F.col("w.opened_on").alias("warehouse_opened_on"),
        F.col("w.city").alias("warehouse_city"),
    )
)

deduped = (
    with_details
    .withColumn("rn", F.row_number().over(w_dedup))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

move_stats = (
    df_stock_moves
    .groupBy("warehouse_id")
    .agg(
        F.count(F.when(F.col("move_type") == "TRANSFER", 1)).alias("move_move_type_count"),
        F.max("moved_at").alias("last_moved_at"),
    )
)

with_counts = (
    deduped.alias("s")
    .join(move_stats.alias("st"), F.col("st.warehouse_id") == F.col("s.warehouse_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("st.move_move_type_count"), F.lit(0)).alias("move_move_type_count"),
        "st.last_moved_at",
    )
)

result = (
    with_counts
    .groupBy("operator")
    .pivot("warehouse_city", ["Singapore", "Bangkok"])
    .agg(F.sum("move_move_type_count"))
    .orderBy("operator")
)
