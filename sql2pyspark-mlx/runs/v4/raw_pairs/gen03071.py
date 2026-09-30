from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("account_type", "customer_id").orderBy("balance", "id")
w2 = Window.partitionBy("account_type").orderBy(F.col("opened_date").desc(), "id")
w3 = Window.partitionBy("customer_id").orderBy(F.col("balance_scaled_rounded").desc(), "id")
w4 = Window.partitionBy("customer_id")

cleaned = (
    df_accounts
    .filter(F.col("status").isNotNull())
    .select(
        "id",
        "customer_id",
        "account_type",
        "balance",
        "opened_date",
        F.round((F.col("balance") * 0.85), 2).alias("balance_scaled"),
        F.upper("status").alias("status_upper"),
        F.coalesce("status", F.lit("OTHER")).alias("status_filled"),
    )
)

filtered = (
    cleaned
    .filter((F.col("opened_date") > F.lit("2023-04-28").cast("date")) & F.col("customer_id").isNotNull() & (F.col("balance") >= 49000))
    .select("*", F.round("balance_scaled", 1).alias("balance_scaled_rounded"))
)

metrics = (
    filtered
    .withColumn("balance_bucket", F.ntile(5).over(w))
    .withColumn("event_seq", F.row_number().over(w2))
    .withColumn("balance_scaled_rounded_bucket", F.ntile(5).over(w3))
    .withColumn("customer_id_stddev_samp_balance", F.stddev_samp("balance").over(w4))
)

matched = (
    metrics
    .alias("e")
    .join(df_subscriptions.alias("sub"), (F.col("e.customer_id") == F.col("sub.customer_id")) & F.col("e.opened_date").between(F.col("sub.start_date"), F.col("sub.end_date")), "left")
    .select(
        "e.*",
        F.col("sub.id").alias("subscription_id"),
        F.col("sub.plan"),
        F.col("sub.monthly_fee"),
    )
)

result = (
    matched
    .groupBy("plan")
    .agg(
        F.max("balance_scaled_rounded_bucket").alias("max_balance_scaled_rounded_bucket"),
        F.percentile_approx("balance_scaled_rounded_bucket", [0.25, 0.5, 0.75]).alias("balance_scaled_rounded_bucket_quartiles"),
    )
    .filter(F.col("max_balance_scaled_rounded_bucket") >= 0)
)
