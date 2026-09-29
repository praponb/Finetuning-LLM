from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("launch_date").between("2024-09-01", "2024-10-01"))
    .groupBy(
        F.when(F.col("brand").isin(["Stark", "Acme"]), "priority").otherwise("other").alias("brand_group"),
    )
    .agg(
        F.max("launch_date").alias("latest_launch_date"),
        F.min("price").alias("min_price"),
    )
)
