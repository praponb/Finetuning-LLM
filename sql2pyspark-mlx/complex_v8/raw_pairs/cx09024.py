from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_dedup = Window.partitionBy("sku_id").orderBy(F.col("moved_at").desc(), F.col("id").desc())

prepared = (
    df_stock_moves
    .filter((F.col("operator").isin("op_bee", "op_ann")) & (~F.col("move_type").isin("OUTBOUND", "TRANSFER")))
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "qty",
        "moved_at",
        "operator",
        "unit_price",
        F.coalesce(F.col("unit_price"), F.lit(0.0)).alias("unit_price_filled"),
    )
)

latest_per_key = (
    prepared
    .withColumn("recency_rank", F.row_number().over(w_dedup))
    .filter(F.col("recency_rank") == 1)
    .drop("recency_rank")
)

result = (
    latest_per_key
    .select("id", "unit_price_filled", "unit_price")
    .unpivot(
        ids=["id"],
        values=["unit_price_filled", "unit_price"],
        variableColumnName="field",
        valueColumnName="amount",
    )
)
