from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("name").like("Ja%"))
    .select("country", F.lit("customer").alias("source"))
    .unionByName(df_sessions.select("country", F.lit("session").alias("source")))
    .orderBy(F.col("source").desc())
)
