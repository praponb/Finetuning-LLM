from pyspark.sql import functions as F

result = (
    df_sessions
    .filter(F.col("customer_id").isNull() & F.col("customer_id").isin([15, 25, 26]))
    .withColumn("country_device", F.concat("country", F.lit("/"), "device"))
    .orderBy(F.col("id").desc())
)
