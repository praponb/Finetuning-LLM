from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy("monthly_fee")
w2 = Window.partitionBy("customer_id").orderBy(F.col("monthly_fee_stddev").desc())

ranked = (
    df_subscriptions
    .withColumn("rank_in_group", F.rank().over(w))
    .filter(F.col("rank_in_group") == 1)
    .drop("rank_in_group")
)

result = (
    ranked
    .groupBy("customer_id", "status")
    .agg(
        F.stddev_samp("monthly_fee").alias("monthly_fee_stddev"),
        F.stddev_samp("monthly_fee").alias("monthly_fee_stddev_2"),
        F.sum(F.when(F.col("plan") == "basic", F.col("monthly_fee")).otherwise(0)).alias("basic_monthly_fee"),
        F.count_if(F.col("plan") == "enterprise").alias("enterprise_events"),
        F.count(F.when(F.col("status") == "EXPIRED", 1)).alias("expired_count"),
    )
    .withColumn("monthly_fee_stddev_rank", F.rank().over(w2))
)
