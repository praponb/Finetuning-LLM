from pyspark.sql import functions as F

result = (
    df_customers
    .select("country", F.lit("customer").alias("source"))
    .unionByName(df_sessions.select("country", F.lit("session").alias("source")))
    .distinct()
    .orderBy(F.col("source").desc())
)
