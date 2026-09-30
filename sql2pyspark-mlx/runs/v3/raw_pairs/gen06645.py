from pyspark.sql import functions as F

base = (
    df_products
    .filter((F.col("price") > 610) & ~F.col("brand").isin(["Umbrella", "Acme"]))
)

agg = (
    base
    .groupBy("brand")
    .agg(
        F.count("*").alias("row_count"),
        F.countDistinct("price").alias("unique_brands"),
    )
)

result = agg.filter(F.col("unique_brands") > 8)
