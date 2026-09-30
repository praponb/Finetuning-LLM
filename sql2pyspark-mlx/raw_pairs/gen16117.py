from pyspark.sql import functions as F

result = (
    df_products
    .groupBy("brand")
    .agg(
        F.sum(F.when(F.col("category") == "Sports", 1).otherwise(0)).alias("sports_count"),
        F.countDistinct("cost").alias("unique_brands"),
        F.countDistinct("stock_qty").alias("unique_suppliers"),
    )
)
