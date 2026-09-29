from pyspark.sql import functions as F

valid = (
    df_accounts
    .filter(F.col("customer_id").isNotNull())
    .select("*", F.date_add("opened_date", 19).alias("opened_date_plus_19d"))
)

enriched = (
    valid
    .alias("s")
    .join(df_customers.alias("p"), F.col("s.customer_id") == F.col("p.id"), "inner")
    .select("s.*", F.col("p.credit_limit"), F.col("p.segment"), F.col("p.signup_date"))
)

kept = (
    enriched
    .alias("s")
    .join(df_transactions.alias("c"), (F.col("c.account_id") == F.col("s.id")) & (F.col("c.is_flagged")), "left_anti")
)

joined = (
    kept
    .alias("src")
    .join(df_customers.alias("d"), F.col("src.customer_id") == F.col("d.id"), "left")
    .select("src.*", F.col("d.country"), F.col("d.segment").alias("customer_segment"))
)

kept_keys = (
    joined
    .select("customer_id", "status")
    .intersectAll(
        df_accounts
        .filter(F.col("balance") < 28000)
        .select("customer_id", "status")
    )
)

kept_rows = (
    joined
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.customer_id") == F.col("k.customer_id")) & (F.col("s.status") == F.col("k.status")), "inner")
    .select("s.*")
)

result = (
    kept_rows
    .alias("src")
    .join(df_customers.alias("p"), F.col("src.customer_id") == F.col("p.id"), "left")
    .groupingSets(
        [["customer_segment", "account_customer_segment"], ["customer_segment"]],
        F.col("p.segment").alias("customer_segment"),
        F.col("src.customer_segment").alias("account_customer_segment"),
    )
    .agg(
        F.count("*").alias("rows_total"),
        F.min(F.col("src.credit_limit")).alias("min_credit_limit"),
        F.grouping_id().alias("gid"),
    )
)
