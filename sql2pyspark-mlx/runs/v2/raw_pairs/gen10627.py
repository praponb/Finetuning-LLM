from pyspark.sql import functions as F

base = (
    df_order_items
    .filter(F.col("quantity").between(3, 4) & (F.col("product_id").isNull() | F.col("quantity").between(3, 5)))
)

product_id_summary = (
    base
    .groupBy(
        "product_id",
        F.when(F.col("unit_price") >= 300, "high").when(F.col("unit_price") >= 70, "mid").otherwise("low").alias("unit_price_bucket"),
    )
    .agg(
        F.min("unit_price").alias("min_unit_price"),
        F.count("*").alias("num_order_items"),
    )
)

result = (
    product_id_summary
    .select("product_id", "unit_price_bucket", "min_unit_price", "num_order_items")
    .orderBy("num_order_items", F.col("product_id").desc(), "unit_price_bucket")
    .limit(10)
)
