from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("name").isNull())
    .select(F.lit("customer").alias("source"), "country")
    .unionByName(df_sessions.select(F.lit("session").alias("source"), "country"))
)
