from pyspark.sql import functions as F

base = df_order_items.filter(F.col("order_id") == 12)

summary = (
    base
    .groupBy(
        F.when(F.col("quantity") >= 7, "high").when(F.col("quantity") >= 3, "mid").otherwise("low").alias("quantity_bucket"),
        "product_id",
    )
    .agg(F.sum("unit_price").alias("total_unit_price"))
)

result = (
    summary
    .select("quantity_bucket", "product_id", "total_unit_price")
    .orderBy("product_id", "quantity_bucket")
    .limit(1)
)
