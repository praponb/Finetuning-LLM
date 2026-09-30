from pyspark.sql import functions as F

src = df_store_sales.filter((F.col("revenue") >= 3000) & (F.col("store_id") == 4))

agg = (
    src
    .groupBy("product_id")
    .agg(F.max("units").alias("max_units"))
)

result = agg.filter(F.col("max_units") > 22)
