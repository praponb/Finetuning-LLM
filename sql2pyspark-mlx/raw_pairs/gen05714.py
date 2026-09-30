from pyspark.sql import functions as F

filtered = (
    df_accounts
    .filter(F.col("status").isNotNull() & (~F.col("status").eqNullSafe(F.lit("FROZEN"))))
    .select(
        "id",
        "customer_id",
        "account_type",
        "balance",
        "status",
        F.add_months("opened_date", 3).alias("opened_date_plus_months"),
        F.initcap(F.trim("status")).alias("status_title"),
    )
)

result = (
    filtered
    .groupBy("account_type")
    .pivot("status", ["FROZEN", "OPEN"])
    .agg(F.max("balance").alias("value_sum"), F.count(F.lit(1)).alias("cnt"))
)
