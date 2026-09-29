from pyspark.sql import functions as F

result = (
    df_products
    .groupBy("category")
    .agg(
        F.countDistinct("stock_qty").alias("unique_brands"),
        F.round(((100 * F.sum(F.when(F.col("category") == "Electronics", 1).otherwise(0))) / F.count("*")), 1).alias("pct_electronics"),
    )
    .orderBy("category")
)
