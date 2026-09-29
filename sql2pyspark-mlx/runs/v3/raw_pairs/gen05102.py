from pyspark.sql import functions as F

src = (
    df_store_sales
    .filter(F.col("store_id").isin([1, 3, 4, 5]) & (F.col("product_id") == 10))
)

agg = (
    src
    .groupBy("product_id")
    .agg(F.sum("units").alias("total_units"))
)

result = (
    agg
    .filter(F.col("total_units") > 200)
    .select("product_id", "total_units")
    .orderBy(F.col("total_units").desc())
)
