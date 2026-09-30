from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy("unit_price_rounded", "id")
w2 = Window.partitionBy("order_id").orderBy(F.col("quantity").desc(), "id")
cost_by_order_id = df_shipments.groupBy("order_id").agg(F.max("cost").alias("max_shipment_cost"))

base = (
    df_order_items
    .filter(F.col("product_id").isNotNull() & F.col("product_id").isNotNull())
    .select(
        "id",
        "order_id",
        "product_id",
        "quantity",
        "unit_price",
        F.round("unit_price", 0).alias("unit_price_rounded"),
        F.col("unit_price").cast("int").alias("unit_price_int"),
    )
)

ranked = (
    base
    .withColumn("dedup_rank", F.row_number().over(w))
    .filter(F.col("dedup_rank") == 1)
    .drop("dedup_rank")
)

deduped = (
    ranked
    .withColumn("dedup_rank", F.row_number().over(w2))
    .filter(F.col("dedup_rank") == 1)
)

summary = (
    deduped
    .groupBy(
        "order_id",
        F.when(F.col("quantity") >= 7, "HIGH").otherwise("LOW").alias("quantity_tier"),
    )
    .agg(
        F.round(F.avg("unit_price_rounded"), 2).alias("avg_unit_price_rounded"),
        F.avg("unit_price").alias("avg_unit_price"),
    )
)

result = (
    summary
    .join(cost_by_order_id, on="order_id", how="left")
    .select(
        "order_id",
        "quantity_tier",
        "avg_unit_price_rounded",
        "avg_unit_price",
        F.col("max_shipment_cost"),
    )
    .orderBy(F.col("avg_unit_price").desc())
)
