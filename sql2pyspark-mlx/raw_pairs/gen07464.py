from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("account_type").orderBy(F.col("abs_balance").desc_nulls_last(), "id")
w2 = Window.partitionBy("status").orderBy(F.col("abs_balance").asc_nulls_first(), "id")
w3 = Window.partitionBy("account_type").orderBy(F.col("age").desc_nulls_last(), "id")
w4 = Window.partitionBy("customer_id").orderBy(F.col("total_age").desc())
amount_by_customer_id = df_orders.groupBy("customer_id").agg(F.avg("amount").alias("avg_order_amount"))

filtered = (
    df_accounts
    .filter((F.col("opened_date") > F.lit("2023-09-17").cast("date")) & F.col("customer_id").isNotNull())
    .withColumn("abs_balance", F.abs("balance"))
    .withColumn("status_clean", F.coalesce("status", F.lit("UNKNOWN")))
)

latest = (
    filtered
    .withColumn("rank_in_group", F.row_number().over(w))
    .filter(F.col("rank_in_group") == 1)
    .drop("rank_in_group")
)

deduped = (
    latest
    .withColumn("latest_rank", F.row_number().over(w2))
    .filter(F.col("latest_rank") == 1)
)

with_dims = (
    deduped
    .alias("s")
    .join(df_customers.alias("c"), F.col("s.customer_id") == F.col("c.id"), "inner")
    .select(
        "s.id",
        "s.customer_id",
        "s.account_type",
        "s.balance",
        "s.abs_balance",
        F.col("c.age"),
    )
)

deduped_accounts = (
    with_dims
    .withColumn("dedup_rank", F.row_number().over(w3))
    .filter(F.col("dedup_rank") == 1)
    .select("id", "customer_id", "balance", "age")
)

daily = (
    deduped_accounts
    .groupBy(
        "customer_id",
        F.when(F.col("balance") >= 85000, "PREMIUM").otherwise("STANDARD").alias("balance_tier"),
    )
    .agg(
        F.countDistinct("customer_id").alias("distinct_customer_ids"),
        F.stddev_samp("balance").alias("balance_stddev"),
        F.round(F.avg("balance"), 2).alias("avg_balance"),
        F.min("balance").alias("min_balance"),
        F.sum("age").alias("total_age"),
        F.percentile_approx("age", 0.95).alias("age_p95"),
    )
    .withColumn("total_age_percent_rank", F.percent_rank().over(w4))
)

result = (
    daily
    .join(amount_by_customer_id, on="customer_id", how="left")
    .select(
        "customer_id",
        "balance_tier",
        "balance_stddev",
        "min_balance",
        "total_age",
        "total_age_percent_rank",
        F.col("avg_order_amount"),
    )
    .orderBy(F.col("balance_tier").desc())
)
