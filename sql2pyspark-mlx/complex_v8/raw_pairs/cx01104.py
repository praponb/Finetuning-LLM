from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_latest = Window.partitionBy("warehouse_id").orderBy(F.col("unit_price").desc(), F.col("id").desc())

scoped_moves = (
    df_stock_moves
    .filter((F.col("operator").isNotNull()) & (F.col("move_type").isin("TRANSFER", "OUTBOUND")))
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "operator",
        "unit_price",
    )
)

latest = (
    scoped_moves
    .withColumn("recency_rank", F.row_number().over(w_latest))
    .filter(F.col("recency_rank") == 1)
    .drop("recency_rank")
)

detail_rows = (
    latest.alias("s")
    .join(df_stock_moves.alias("st"), F.col("st.sku_id") == F.col("s.sku_id"), "inner")
    .select(
        "s.id",
        "s.warehouse_id",
        "s.sku_id",
        "s.qty",
        "s.unit_price",
        F.col("st.id").alias("move_id"),
        F.col("st.moved_at").alias("move_moved_at"),
        F.col("st.qty").alias("move_qty"),
        F.col("st.move_type").alias("move_move_type"),
    )
)

result = (
    detail_rows
    .filter(F.col("warehouse_id") <= 2)
    .select("move_qty", "sku_id", "qty")
    .distinct()
    .orderBy(F.col("move_qty").asc(), F.col("sku_id").desc())
)
