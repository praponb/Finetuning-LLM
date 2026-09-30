from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("age") < 31)
    .select("country", F.lit("customer").alias("source"))
    .unionByName(
        df_sessions
        .filter(F.col("customer_id") == 15)
        .select("country", F.lit("session").alias("source"))
    )
    .distinct()
    .orderBy("source")
)
