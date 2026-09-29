from pyspark.sql import functions as F

result = (
    df_customers
    .select("country", F.lit("customer").alias("kind"))
    .union(
        df_sessions
        .filter(F.col("country").isNotNull())
        .select("browser", F.lit("session").alias("kind"))
    )
)
