from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("product_name").like("Des%"))
    .groupBy(
        "brand",
        "category",
        F.when(F.col("stock_qty") >= 429, "high").when(F.col("stock_qty") >= 193, "mid").otherwise("low").alias("stock_qty_bucket"),
    )
    .agg(
        F.sum(F.when(F.col("brand") == "Stark", 1).otherwise(0)).alias("stark_count"),
        F.round(((100 * F.sum(F.when(F.col("brand") == "Initech", 1).otherwise(0))) / F.count("*")), 1).alias("pct_initech"),
    )
)
