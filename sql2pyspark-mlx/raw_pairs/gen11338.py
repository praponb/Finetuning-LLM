from pyspark.sql import functions as F

result = (
    df_sessions
    .filter(F.col("country").isNotNull())
    .select("country", F.lit("session").alias("record_type"))
    .union(
        df_customers
        .filter(F.col("name").isNotNull())
        .select("name", F.lit("customer").alias("record_type"))
    )
)
