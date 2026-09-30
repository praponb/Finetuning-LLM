from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.col("city").isin(["Bangkok", "New York"]))
    .select("country", F.lit("supplier").alias("source"))
    .unionByName(
        df_sessions
        .filter(F.col("customer_id") == 20)
        .select("country", F.lit("session").alias("source"))
    )
)
