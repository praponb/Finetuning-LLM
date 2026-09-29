from pyspark.sql import functions as F

base = (
    df_accounts
    .filter((~F.col("status").eqNullSafe(F.lit("CLOSED"))) & F.col("customer_id").isNotNull())
    .select(
        "id",
        "customer_id",
        "account_type",
        "balance",
        "opened_date",
        "status",
        F.abs("balance").alias("abs_balance"),
        F.coalesce("status", F.lit("OTHER")).alias("resolved_status"),
    )
)

result = (
    base
    .groupBy("status")
    .pivot("account_type", ["CREDIT", "SAVINGS"])
    .agg(F.max("abs_balance"))
)
