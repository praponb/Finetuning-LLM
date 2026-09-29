from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy(F.col("monthly_fee").asc_nulls_last())

deduped = (
    df_subscriptions
    .withColumn("rank_in_group", F.rank().over(w))
    .filter(F.col("rank_in_group") == 1)
    .drop("rank_in_group")
)

result = (
    deduped
    .withColumn("monthly_fee_tier", F.when(F.col("monthly_fee") >= 197, "HIGH").otherwise("LOW"))
    .rollup("monthly_fee_tier")
    .agg(
        F.size(F.collect_set("plan")).alias("num_plans"),
        F.round(F.sum("monthly_fee") / F.nullif(F.count(F.when(F.col("plan") == "premium", 1)), F.lit(0)), 2).alias("monthly_fee_per_premium"),
        F.count(F.when(F.col("status") == "CANCELLED", 1)).alias("cancelled_count"),
        F.count_if(F.col("status") == "EXPIRED").alias("expired_events"),
        F.round(F.sum("monthly_fee") / F.nullif(F.count(F.when(F.col("plan") == "enterprise", 1)), F.lit(0)), 2).alias("monthly_fee_per_enterprise"),
        F.avg("monthly_fee").alias("avg_monthly_fee"),
        F.grouping_id().alias("grouping_id"),
    )
    .filter(F.col("cancelled_count") > 1)
    .orderBy("expired_events")
)
