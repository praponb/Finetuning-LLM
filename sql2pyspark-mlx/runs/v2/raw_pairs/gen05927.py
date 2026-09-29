from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("country").isin(["US", "DE"]))
    .select("country", F.lit("customer").alias("source"))
    .unionByName(df_sessions.select("country", F.lit("session").alias("source")))
    .distinct()
    .orderBy("source")
)
