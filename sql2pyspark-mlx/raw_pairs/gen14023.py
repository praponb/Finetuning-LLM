from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.col("sqft") != 4866)
    .select("store_name", F.lit("store").alias("kind"))
    .union(df_employees.select("level", F.lit("employee").alias("kind")))
    .orderBy("kind")
)
