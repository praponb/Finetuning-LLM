from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("cost").between(420, 710) & (F.col("launch_date") < "2025-01-01"))
    .groupBy("brand")
    .agg(
        F.max("price").alias("max_price"),
        F.countDistinct("cost").alias("unique_brands"),
        F.sum("price").alias("total_price"),
    )
    .orderBy(F.col("max_price").desc())
)
