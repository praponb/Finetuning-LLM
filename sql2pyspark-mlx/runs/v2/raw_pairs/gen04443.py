from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("signup_date") > "2025-05-01")
    .select("country", F.lit("customer").alias("source"))
    .unionByName(
        df_sessions
        .filter(F.col("browser").isin(["Edge", "Chrome"]))
        .select("country", F.lit("session").alias("source"))
    )
)
