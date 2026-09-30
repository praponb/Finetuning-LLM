from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy(F.col("start_date").asc_nulls_last())
w2 = Window.partitionBy("plan").orderBy(F.col("rank_in_group").desc(), "id")
w3 = Window.partitionBy("customer_id").orderBy(F.col("monthly_fee").desc(), "id")
w_hist = Window.orderBy(F.col("monthly_fee").asc_nulls_last())

latest = (
    df_subscriptions
    .withColumn("rank_in_group", F.rank().over(w))
    .filter(F.col("rank_in_group") == 1)
)

deduped = (
    latest
    .withColumn("rn", F.row_number().over(w2))
    .filter(F.col("rn") == 1)
)

scored = (
    deduped
    .withColumn("monthly_fee_bucket", F.ntile(5).over(w3))
    .withColumn("monthly_fee_cume_dist", F.cume_dist().over(w_hist))
)

with_dims = (
    scored
    .alias("src")
    .join(df_customers.alias("c"), F.col("src.customer_id") == F.col("c.id"), "left")
    .select("src.*", F.col("c.credit_limit"), F.col("c.email"), F.col("c.segment"))
)

result = (
    with_dims
    .withColumn("monthly_fee_cume_dist_tier", F.when(F.col("monthly_fee_cume_dist") >= 0.75, "PREMIUM").otherwise("STANDARD"))
    .withColumn("monthly_fee_tier", F.when(F.col("monthly_fee") >= 233, "PREMIUM").otherwise("STANDARD"))
    .groupingSets(
        [["monthly_fee_cume_dist_tier", "monthly_fee_tier"], ["monthly_fee_cume_dist_tier"]],
        "monthly_fee_cume_dist_tier",
        "monthly_fee_tier",
    )
    .agg(
        F.avg(F.when(F.col("status") == "CANCELLED", F.col("monthly_fee"))).alias("cancelled_avg_monthly_fee"),
        F.sum(F.when(F.col("segment") == "Consumer", F.col("monthly_fee")).otherwise(0)).alias("consumer_monthly_fee"),
        F.sum(F.when(F.col("segment") == "SMB", F.col("credit_limit"))).alias("smb_total_credit_limit"),
        F.round(F.avg("credit_limit"), 2).alias("avg_credit_limit"),
        F.count(F.when(F.col("status") == "ACTIVE", 1)).alias("active_count"),
        F.count("*").alias("num_rows"),
        F.grouping("monthly_fee_cume_dist_tier").alias("grouping_id"),
    )
    .filter(F.col("consumer_monthly_fee") < 0)
)
