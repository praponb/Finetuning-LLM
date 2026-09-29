from pyspark.sql import functions as F

base = (
    df_accounts
    .filter((F.col("opened_date") >= F.lit("2023-08-27").cast("date")) & (F.col("account_type") == "CREDIT"))
    .select(
        "*",
        F.col("balance").cast("int").alias("balance_int"),
        F.coalesce("status", F.lit("NONE")).alias("resolved_status"),
    )
)

enriched = (
    base
    .alias("src")
    .join(df_customers.alias("d"), F.col("src.customer_id") == F.col("d.id"), "left")
    .filter(F.col("d.segment").isin(["SMB", "Consumer"]))
    .select(
        "src.id",
        "src.customer_id",
        "src.balance",
        "src.opened_date",
        "src.balance_int",
        "src.resolved_status",
        F.col("d.credit_limit"),
        F.col("d.email"),
        F.col("d.segment"),
    )
)

filtered = (
    enriched
    .filter(F.month("opened_date").isin([2, 8, 9]))
    .select(
        "id",
        "customer_id",
        "opened_date",
        "balance_int",
        "resolved_status",
        "credit_limit",
        "email",
        F.round((F.col("credit_limit") * 0.9), 2).alias("credit_limit_adjusted"),
        F.coalesce("segment", F.lit("DIRECT")).alias("segment_clean"),
    )
)

with_dims = (
    filtered
    .alias("a")
    .join(df_customers.alias("p"), F.col("a.customer_id") == F.col("p.id"), "inner")
    .select(
        "a.id",
        "a.customer_id",
        "a.credit_limit_adjusted",
        F.col("p.email").alias("customer_email"),
    )
)

eligible = (
    with_dims
    .alias("s")
    .join(df_transactions.alias("ch"), (F.col("ch.account_id") == F.col("s.id")) & (F.col("ch.account_id").isin([16, 29])), "left_semi")
)

result = (
    eligible
    .groupBy("customer_id")
    .agg(
        F.countDistinct("customer_id").alias("distinct_customer_ids"),
        F.min("credit_limit_adjusted").alias("min_credit_limit_adjusted"),
        F.min("credit_limit_adjusted").alias("min_credit_limit_adjusted_2"),
    )
    .filter(F.col("min_credit_limit_adjusted_2") >= 0)
    .orderBy(F.col("min_credit_limit_adjusted").asc_nulls_first())
)
