from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.col("region").isin(["North", "East", "South"]))
    .select("opened_date", F.lit("store").alias("source"))
    .unionByName(
        df_customers
        .select(
        F.col("signup_date").alias("opened_date"),
        F.lit("customer").alias("source"),
    )
    )
    .distinct()
)
