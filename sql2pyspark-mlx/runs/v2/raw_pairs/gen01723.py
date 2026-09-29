from pyspark.sql import functions as F

result = (
    df_products
    .agg(
        F.count("price").alias("price_count"),
        F.round(((100 * F.sum(F.when(F.col("brand") == "Umbrella", 1).otherwise(0))) / F.count("*")), 1).alias("pct_umbrella"),
        F.countDistinct("stock_qty").alias("unique_brands"),
    )
    .orderBy(F.col("pct_umbrella").desc())
)
