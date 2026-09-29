from pyspark.sql import functions as F

result = (
    df_customers
    .select("country", F.lit("customer").alias("source"))
    .unionByName(
        df_sessions
        .filter(F.col("browser") != "Safari")
        .select("country", F.lit("session").alias("source"))
    )
    .distinct()
)
