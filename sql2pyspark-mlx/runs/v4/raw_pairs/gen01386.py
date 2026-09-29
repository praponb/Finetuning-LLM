from pyspark.sql import functions as F

with_dims = (
    df_accounts
    .alias("s")
    .join(df_customers.alias("p"), F.col("s.customer_id") == F.col("p.id"), "left")
    .select("s.*", F.col("p.name"), F.col("p.country"), F.col("p.signup_date"))
)

result = (
    with_dims
    .withColumn("balance_tier", F.when(F.col("balance") >= 75000, "HIGH").otherwise("LOW"))
    .cube("account_type", "balance_tier")
    .agg(
        F.count_if(F.col("account_type") == "SAVINGS").alias("savings_events"),
        F.countDistinct("country").alias("distinct_countries"),
        F.max("balance").alias("max_balance"),
        F.grouping_id().alias("gid"),
    )
    .orderBy(F.col("max_balance").desc_nulls_last())
)
