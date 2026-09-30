from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.col("sqft") >= 15874)
    .orderBy("id", "store_name")
)
