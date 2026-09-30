from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.start_date").desc(), F.col("c.id").desc())
w2 = Window.partitionBy("segment").orderBy("signup_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.orderBy(F.col("credit_limit").asc_nulls_first())
w4 = Window.partitionBy("subscription_plan").orderBy("credit_limit", "id").rowsBetween(-5, Window.currentRow)
w5 = Window.partitionBy("segment")
w6 = Window.partitionBy("subscription_plan").orderBy(F.col("signup_date").desc(), "id")
w7 = Window.partitionBy("segment").orderBy(F.col("signup_date").desc_nulls_last(), "id")
w8 = Window.partitionBy("country", "city").orderBy(F.col("subscription_monthly_fee").asc_nulls_last(), "id")
w9 = Window.partitionBy("country").orderBy("subscription_monthly_fee_avg_5", "id")
w10 = Window.partitionBy("city").orderBy(F.col("subscription_monthly_fee").desc(), "id")
w11 = Window.partitionBy("segment", "subscription_plan").orderBy("dedup_rank", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

with_last = (
    df_customers
    .alias("s")
    .join(df_subscriptions.alias("c"), (F.col("c.customer_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "s.*",
        F.col("c.plan").alias("subscription_plan"),
        F.col("c.monthly_fee").alias("subscription_monthly_fee"),
    )
)

scored = (
    with_last
    .withColumn("first_known_credit_limit", F.first("credit_limit", ignorenulls=True).over(w2))
    .withColumn("credit_limit_cume_dist", F.cume_dist().over(w3))
    .withColumn("subscription_monthly_fee_avg_5", F.avg("subscription_monthly_fee").over(w4))
    .withColumn("segment_count_subscription_monthly_fee", F.count("subscription_monthly_fee").over(w5))
)

deduped = (
    scored
    .withColumn("dedup_rank", F.row_number().over(w6))
    .filter(F.col("dedup_rank") == 1)
)

filtered = (
    deduped
    .filter(F.col("city").isNotNull())
    .withColumn("credit_limit_plus_credit_limit_cume_dist", (F.col("credit_limit") + F.col("credit_limit_cume_dist")))
    .withColumn("segment_filled", F.coalesce("segment", F.lit("N/A")))
)

deduped_customers = (
    filtered
    .withColumn("rank_in_group", F.row_number().over(w7))
    .filter(F.col("rank_in_group") == 1)
    .drop("rank_in_group")
)

deduped_customers_2 = (
    deduped_customers
    .withColumn("rank_in_group", F.row_number().over(w8))
    .filter(F.col("rank_in_group") == 1)
    .drop("rank_in_group")
)

result = (
    deduped_customers_2
    .withColumn("next_subscription_monthly_fee_avg_5", F.lead("subscription_monthly_fee_avg_5", 1, 0).over(w9))
    .withColumn("subscription_monthly_fee_bucket", F.ntile(10).over(w10))
    .withColumn("last_known_subscription_monthly_fee", F.last("subscription_monthly_fee", ignorenulls=True).over(w11))
)
