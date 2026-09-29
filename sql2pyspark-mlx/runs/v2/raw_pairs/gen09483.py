from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("units").isNull() & F.col("store_id").isin([1, 4, 5, 6]))
    .select("id", "revenue")
    .orderBy("revenue", F.col("id").desc())
)
