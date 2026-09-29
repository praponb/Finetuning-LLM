from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("monthly_fee").desc_nulls_first())
w2 = Window.orderBy(F.col("monthly_fee").asc_nulls_first())
w3 = Window.partitionBy("customer_id")
w4 = Window.partitionBy("plan", "customer_id")
w5 = Window.orderBy(F.col("monthly_fee_percent_rank").desc())
w6 = Window.partitionBy("customer_id", "plan").orderBy(F.col("monthly_fee_cume_dist_2").desc_nulls_last())
balance_by_customer_id = df_accounts.groupBy("customer_id").agg(F.avg("balance").alias("avg_account_balance"))

remaining_keys = (
    df_subscriptions
    .select("status", "id")
    .exceptAll(
        df_subscriptions
        .filter(F.col("start_date") >= "2025-04-01")
        .select("status", "id")
    )
)

reconciled = (
    df_subscriptions
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.status") == F.col("k.status")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

scored = (
    reconciled
    .select(
        "customer_id",
        "plan",
        "start_date",
        "end_date",
        "monthly_fee",
        F.cume_dist().over(w).alias("monthly_fee_cume_dist"),
        F.percent_rank().over(w2).alias("monthly_fee_percent_rank"),
        F.min("monthly_fee").over(w3).alias("customer_id_min_monthly_fee"),
        F.avg("monthly_fee").over(w4).alias("plan_avg_monthly_fee"),
        F.cume_dist().over(w2).alias("monthly_fee_cume_dist_2"),
    )
)

valid = (
    scored
    .filter((F.col("monthly_fee") >= 180) & (F.col("start_date") >= F.lit("2023-09-07").cast("date")))
    .select(
        "customer_id",
        "plan",
        "start_date",
        "monthly_fee",
        "monthly_fee_cume_dist",
        "monthly_fee_percent_rank",
        "customer_id_min_monthly_fee",
        "plan_avg_monthly_fee",
        "monthly_fee_cume_dist_2",
        F.date_format("end_date", "yyyy-MM").alias("end_date_label"),
    )
)

filtered = (
    valid
    .filter(F.col("monthly_fee_percent_rank").between(0.48, 0.51))
    .select("*", F.length("plan").alias("plan_length"))
)

scored_subscriptions = (
    filtered
    .select(
        "*",
        F.cume_dist().over(w5).alias("monthly_fee_percent_rank_cume_dist"),
        F.dense_rank().over(w6).alias("monthly_fee_cume_dist_2_dense_rank"),
    )
)

joined = (
    scored_subscriptions
    .alias("e")
    .join(df_customers.alias("d"), F.col("e.customer_id") == F.col("d.id"), "inner")
    .select("e.*", F.col("d.credit_limit"), F.col("d.country"), F.col("d.email"))
)

result = (
    joined
    .join(balance_by_customer_id, on="customer_id", how="left")
    .select(
        "customer_id",
        "start_date",
        "monthly_fee",
        "customer_id_min_monthly_fee",
        "plan_avg_monthly_fee",
        "monthly_fee_cume_dist_2",
        "plan_length",
        "monthly_fee_percent_rank_cume_dist",
        "monthly_fee_cume_dist_2_dense_rank",
        F.col("avg_account_balance"),
    )
    .orderBy(F.col("plan_avg_monthly_fee").asc_nulls_first())
)
