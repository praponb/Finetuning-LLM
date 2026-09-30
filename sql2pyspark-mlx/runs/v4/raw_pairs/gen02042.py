from pyspark.sql import functions as F

base = df_store_sales.filter(F.col("product_id").isin([1, 3, 9]))

agg = (
    base
    .groupBy("store_id")
    .agg(
        F.count("*").alias("cnt"),
        F.count("revenue").alias("revenue_count"),
    )
)

result = agg.filter(F.col("cnt") < 8)
