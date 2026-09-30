from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy(F.col("amount").desc_nulls_last(), "id")
w2 = Window.partitionBy("order_id").orderBy(F.col("amount_rounded").desc(), "id")
quantity_by_order_id = df_order_items.groupBy("order_id").agg(F.avg("quantity").alias("avg_order_item_quantity"))

base = (
    df_payments
    .filter(F.col("method").isNotNull() & (F.col("paid_at") >= "2023-09-01"))
    .select(
        "id",
        "order_id",
        "amount",
        F.coalesce("method", F.lit("N/A")).alias("method_filled"),
        F.coalesce("method", F.lit("DIRECT")).alias("method_clean"),
    )
)

base_payments = (
    base
    .filter(F.col("order_id").isNotNull() & (F.col("order_id") == 12))
    .select(
        "*",
        F.when(F.col("amount") >= 1690, "High").when(F.col("amount") >= 250, "Medium").otherwise("Low").alias("amount_level"),
        F.round("amount", 2).alias("amount_rounded"),
    )
)

deduped = (
    base_payments
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
)

ranked = (
    deduped
    .withColumn("dedup_rank", F.row_number().over(w2))
    .filter(F.col("dedup_rank") == 1)
)

result = (
    ranked
    .join(quantity_by_order_id, on="order_id", how="left")
    .select(
        "id",
        "order_id",
        "amount",
        "amount_level",
        "rn",
        "dedup_rank",
        F.col("avg_order_item_quantity"),
    )
    .orderBy(
        F.col("avg_order_item_quantity").desc_nulls_last(),
        F.col("dedup_rank").desc(),
    )
)
