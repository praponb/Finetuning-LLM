from pyspark.sql import functions as F

base = df_store_sales.filter(F.col("units") != 13)

agg = (
    base
    .groupBy("store_id", "product_id")
    .agg(
        F.count("revenue").alias("revenue_count"),
        F.count("*").alias("store_sale_count"),
    )
)

result = agg.orderBy("store_id")
