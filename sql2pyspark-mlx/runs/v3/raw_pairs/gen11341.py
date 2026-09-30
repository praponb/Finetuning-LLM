from pyspark.sql import functions as F

result = (
    df_order_items
    .rollup(
        "product_id",
        F.when(F.col("quantity") >= 7, "high").when(F.col("quantity") >= 3, "mid").otherwise("low").alias("quantity_bucket"),
    )
    .agg(F.sum((F.col("unit_price") * F.col("quantity"))).alias("revenue"))
    .orderBy(F.col("revenue").desc(), "quantity_bucket")
)
