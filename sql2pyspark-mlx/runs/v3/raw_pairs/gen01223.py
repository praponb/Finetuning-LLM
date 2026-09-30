from pyspark.sql import functions as F

result = (
    df_store_sales
    .select("id", "units", "store_id", "revenue")
    .orderBy(F.col("units").desc(), "id")
    .limit(5)
)
