from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("signup_date") <= "2024-06-02")
    .select("city", F.lit("customer").alias("source"))
    .unionByName(
        df_stores
        .filter(~F.col("region").isin(["Central", "East", "North"]))
        .select("city", F.lit("store").alias("source"))
    )
    .distinct()
)
