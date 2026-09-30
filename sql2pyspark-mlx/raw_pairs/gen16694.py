from pyspark.sql import functions as F

kept = (
    df_accounts
    .alias("s")
    .join(df_transactions.alias("c"), (F.col("c.account_id") == F.col("s.id")) & (F.col("c.merchant") == "Lazada"), "left_semi")
)

result = (
    kept
    .groupBy(
        F.when(F.col("balance") >= 89000, "LARGE").otherwise("SMALL").alias("balance_tier"),
    )
    .agg(
        F.percentile_approx("balance", 0.9).alias("balance_p90"),
        F.countDistinct("customer_id").alias("distinct_customer_ids"),
    )
    .orderBy(F.col("distinct_customer_ids").asc_nulls_last())
)
