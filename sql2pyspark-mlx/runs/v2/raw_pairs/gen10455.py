from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("stock_qty").between(363, 384))
    .groupBy(
        F.when(F.col("price") >= 1200, "high").when(F.col("price") >= 570, "mid").otherwise("low").alias("price_bucket"),
        "supplier_id",
    )
    .agg(
        F.max("price").alias("max_price"),
        F.max("launch_date").alias("latest_launch_date"),
    )
    .filter(F.col("max_price") < 650)
    .orderBy(F.col("price_bucket").desc(), "supplier_id")
    .limit(1)
)
