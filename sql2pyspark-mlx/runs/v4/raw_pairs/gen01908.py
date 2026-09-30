from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy(F.col("opened_date").desc_nulls_last(), "id")
w2 = Window.partitionBy("customer_id").orderBy(F.col("balance").asc_nulls_first(), "id")
monthly_fee_by_customer_id = df_subscriptions.groupBy("customer_id").agg(F.max("monthly_fee").alias("max_subscription_monthly_fee"))

cleaned = (
    df_accounts
    .filter(F.col("customer_id").isNotNull())
    .withColumn("balance_rounded", F.round("balance", 0))
    .withColumn("status_filled", F.coalesce("status", F.lit("N/A")))
)

ranked = (
    cleaned
    .withColumn("rank_in_group", F.row_number().over(w))
    .filter(F.col("rank_in_group") == 1)
)

deduped = (
    ranked
    .withColumn("dedup_rank", F.row_number().over(w2))
    .filter(F.col("dedup_rank") == 1)
)

wide = (
    deduped
    .groupBy("customer_id")
    .pivot("account_type", ["CREDIT", "CHECKING"])
    .agg(F.sum("balance_rounded"))
    .select(
        "customer_id",
        F.col("CREDIT").alias("credit"),
        F.col("CHECKING").alias("checking"),
    )
)

result = (
    wide
    .join(monthly_fee_by_customer_id, on="customer_id", how="left")
    .select("customer_id", "credit", "checking", F.col("max_subscription_monthly_fee"))
    .orderBy(F.col("customer_id").desc_nulls_last())
    .limit(20)
)
