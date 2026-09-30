from pyspark.sql import functions as F

result = (
    df_customers
    .select("signup_date", F.lit("customer").alias("source"))
    .unionByName(
        df_inventory
        .filter(F.col("quantity") >= 175)
        .select(
        F.col("last_restock").alias("signup_date"),
        F.lit("inventory").alias("source"),
    )
    )
    .orderBy(F.col("signup_date").desc())
)
