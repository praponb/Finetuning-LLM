from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("unit_price") >= 150)
    .groupBy(
        "product_id",
        F.when(F.col("quantity") >= 7, "high").when(F.col("quantity") >= 4, "mid").otherwise("low").alias("quantity_bucket"),
    )
    .agg(F.sum((F.col("unit_price") * F.col("quantity"))).alias("revenue"))
    .filter(F.col("revenue") > 21)
    .select("product_id", "quantity_bucket", "revenue")
    .orderBy(F.col("product_id").desc())
)
