from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_dedup = Window.partitionBy("operator").orderBy("moved_at_day", "id")

base_moves = (
    df_stock_moves
    .filter(F.col("warehouse_id").isNotNull())
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "moved_at",
        "operator",
        "unit_price",
        F.date_trunc("day", F.col("moved_at")).alias("moved_at_day"),
    )
)

deduped = (
    base_moves
    .withColumn("row_num", F.row_number().over(w_dedup))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

with_parent = (
    deduped.alias("s")
    .join(df_skus.alias("sk"), F.col("sk.id") == F.col("s.sku_id"), "inner")
    .select(
        "s.id",
        "s.warehouse_id",
        "s.sku_id",
        "s.moved_at",
        "s.operator",
        "s.moved_at_day",
        F.col("sk.is_active").alias("sku_is_active"),
        F.col("sk.unit_cost").alias("sku_unit_cost"),
        F.col("sk.labels").alias("sku_labels"),
    )
)

result = (
    with_parent
    .filter(F.col("moved_at") >= F.lit("2025-01-13 09:00:00").cast("timestamp"))
    .select(
        "id",
        "moved_at",
        "sku_is_active",
        "sku_unit_cost",
        F.datediff(F.lit("2025-12-31").cast("date"), F.col("moved_at_day")).alias("days_since_moved_at_day"),
    )
)
