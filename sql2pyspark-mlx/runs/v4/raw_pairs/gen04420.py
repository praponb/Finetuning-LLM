from pyspark.sql import functions as F

base = df_store_sales.filter(F.col("revenue") > 1300)

agg = (
    base
    .groupBy("store_id")
    .agg(
        F.count("units").alias("units_count"),
        F.countDistinct("revenue").alias("unique_products"),
    )
)

result = (
    agg
    .filter(F.col("unique_products") < 8)
    .orderBy("unique_products")
)
