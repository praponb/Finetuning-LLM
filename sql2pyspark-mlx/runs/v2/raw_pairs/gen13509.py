from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("credit_limit").between(4500, 13200))
    .select("country", F.lit("customer").alias("source"))
    .unionByName(
        df_sessions
        .filter(F.col("browser").isin(["Edge", "Safari"]))
        .select("country", F.lit("session").alias("source"))
    )
)
