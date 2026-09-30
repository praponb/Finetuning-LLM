from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy("start_date", "id").rowsBetween(-3, Window.currentRow)
w2 = Window.partitionBy("status").orderBy("end_date", "id").rowsBetween(-2, -1)
w3 = Window.partitionBy("status").orderBy("monthly_fee", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.orderBy("monthly_fee", "id")
w5 = Window.partitionBy("plan").orderBy(F.col("monthly_fee_avg_2_prior").desc_nulls_first())
w6 = Window.partitionBy("customer_id", "plan")
w7 = Window.partitionBy("status")

metrics = (
    df_subscriptions
    .withColumn("monthly_fee_avg_3", F.avg("monthly_fee").over(w))
    .withColumn("monthly_fee_avg_2_prior", F.avg("monthly_fee").over(w2))
    .withColumn("running_monthly_fee", F.sum("monthly_fee").over(w3))
    .withColumn("monthly_fee_bucket", F.ntile(4).over(w4))
)

deduped = (
    metrics
    .withColumn("latest_rank", F.rank().over(w5))
    .filter(F.col("latest_rank") == 1)
)

scored = (
    deduped
    .withColumn("monthly_fee_avg_2_prior_share", F.round((F.col("monthly_fee_avg_2_prior") / F.sum("monthly_fee_avg_2_prior").over(w6)), 4))
    .withColumn("status_sum_running_monthly_fee", F.sum("running_monthly_fee").over(w7))
)

kept_keys = (
    scored
    .select("customer_id", "status")
    .intersect(
        df_subscriptions
        .filter(~F.col("status").isin(["PAUSED", "CANCELLED"]))
        .select("customer_id", "status")
    )
)

verified = (
    scored
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.customer_id") == F.col("k.customer_id")) & (F.col("s.status") == F.col("k.status")), "inner")
    .select("s.*")
)

result = (
    verified
    .groupBy(F.date_trunc("QUARTER", "start_date").alias("quarter_start"))
    .agg(
        F.sum(F.when(F.col("plan") == "basic", F.col("monthly_fee_bucket"))).alias("basic_total_monthly_fee_bucket"),
        F.count(F.when(F.col("plan") == "basic", 1)).alias("basic_count"),
    )
    .orderBy(F.col("basic_total_monthly_fee_bucket").desc())
)
