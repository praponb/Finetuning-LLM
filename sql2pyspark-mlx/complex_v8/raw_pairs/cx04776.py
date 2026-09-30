from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_latest = Window.partitionBy("sku_id", "warehouse_id").orderBy(F.col("qty").desc(), F.col("id").desc())
w_dedup = Window.partitionBy("warehouse_id").orderBy(F.col("qty").desc(), F.col("id").desc())

prepared = (
    df_stock_moves
    .filter(F.col("move_type") != "INBOUND")
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "qty",
        "moved_at",
        "operator",
        "unit_price",
        F.lower(F.trim(F.col("move_type"))).alias("move_type_clean"),
        F.greatest(F.col("unit_price"), F.col("qty")).alias("greatest_unit_price_qty"),
    )
)

latest_per_key = (
    prepared
    .withColumn("row_num", F.row_number().over(w_latest))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

latest = (
    latest_per_key
    .withColumn("recency_rank", F.row_number().over(w_dedup))
    .filter(F.col("recency_rank") == 1)
    .drop("recency_rank")
)

result = (
    latest
    .select("id", "unit_price", "greatest_unit_price_qty")
    .unpivot(
        ids=["id"],
        values=["unit_price", "greatest_unit_price_qty"],
        variableColumnName="metric_name",
        valueColumnName="amount",
    )
    .filter(F.col("amount").isNotNull())
)
