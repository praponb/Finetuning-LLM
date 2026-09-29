from pyspark.sql import functions as F

base = df_order_items.filter(F.col("unit_price").between(190, 300))

order_id_summary = (
    base
    .groupBy(
        "order_id",
        F.when(F.col("quantity") >= 8, "high").when(F.col("quantity") >= 4, "mid").otherwise("low").alias("quantity_bucket"),
    )
    .agg(F.count("unit_price").alias("unit_price_count"))
)

result = (
    order_id_summary
    .select("order_id", "quantity_bucket", "unit_price_count")
    .orderBy("quantity_bucket", "order_id")
    .limit(5)
)
