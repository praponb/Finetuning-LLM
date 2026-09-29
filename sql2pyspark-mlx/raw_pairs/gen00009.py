from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("product_name").isNotNull())
    .agg(
        F.count("*").alias("cnt"),
        F.countDistinct("stock_qty").alias("unique_suppliers"),
    )
    .orderBy(F.col("cnt").desc())
)
