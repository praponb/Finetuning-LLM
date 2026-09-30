from pyspark.sql import functions as F

filtered = (
    df_accounts
    .filter(F.col("opened_date") > F.lit("2023-07-11").cast("date"))
    .select(
        "id",
        "customer_id",
        "account_type",
        "balance",
        "opened_date",
        "status",
        F.round("balance", 0).alias("balance_rounded"),
    )
)

result = (
    filtered
    .groupBy(
        F.when(F.col("balance_rounded") >= 87, "HIGH").otherwise("LOW").alias("balance_rounded_tier"),
    )
    .agg(
        F.percentile_approx("balance_rounded", [0.25, 0.5, 0.75]).alias("balance_rounded_quartiles"),
        F.countDistinct("customer_id").alias("distinct_customer_ids"),
        F.sum(F.when(F.col("status") == "FROZEN", F.col("balance_rounded")).otherwise(0)).alias("frozen_balance_rounded"),
    )
)
