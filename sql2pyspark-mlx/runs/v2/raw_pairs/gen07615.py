from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.col("sqft") <= 9813)
    .select("id", "city")
    .orderBy("city")
)
