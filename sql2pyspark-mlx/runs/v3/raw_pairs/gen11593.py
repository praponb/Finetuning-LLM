from pyspark.sql import functions as F

base = (
    df_accounts
    .filter(((F.col("account_type") == "SAVINGS") & F.col("status").isin(["FROZEN", "CLOSED", "OPEN"])) | F.col("customer_id").isin([4, 5, 35]))
)

agg = (
    base
    .groupBy("status")
    .agg(
        F.round(((100 * F.sum(F.when(F.col("status") == "CLOSED", 1).otherwise(0))) / F.count("*")), 1).alias("pct_closed"),
    )
)

result = (
    agg
    .filter(F.col("pct_closed") < 76)
    .select("status", "pct_closed")
    .orderBy(F.col("pct_closed").desc())
)
