from pyspark.sql import functions as F

result = (
    df_customers
    .select("country", F.lit("customer").alias("origin"))
    .unionByName(df_sessions.select("country", F.lit("session").alias("origin")))
    .distinct()
)
