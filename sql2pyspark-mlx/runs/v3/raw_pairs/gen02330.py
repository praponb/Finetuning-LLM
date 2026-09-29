from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.col("store_name").isNotNull() & (F.col("city") == "London"))
    .select("id", "city")
    .orderBy(F.col("id").desc())
)
