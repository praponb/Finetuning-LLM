from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("city") != "Singapore")
    .select(F.lit("customer").alias("kind"), "country")
    .unionByName(
        df_sessions
        .filter(F.col("country") == "DE")
        .select(F.lit("session").alias("kind"), "country")
    )
    .orderBy(F.col("country").desc())
)
