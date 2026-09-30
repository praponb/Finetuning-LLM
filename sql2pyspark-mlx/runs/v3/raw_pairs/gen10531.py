from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("price") <= 940)
    .groupBy(
        "brand",
        F.when(F.col("brand").isin(["Acme", "Initech"]), "core").otherwise("other").alias("brand_group"),
    )
    .agg(F.count("price").alias("price_count"))
    .filter(F.col("price_count") >= 3)
)
