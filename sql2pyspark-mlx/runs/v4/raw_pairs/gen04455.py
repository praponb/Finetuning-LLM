from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("plan").orderBy("monthly_fee", "id")
w2 = Window.partitionBy("status").orderBy("monthly_fee", "id")
w3 = Window.partitionBy("status", "customer_id").orderBy("monthly_fee", "id")
w4 = Window.partitionBy("customer_id", "status")
w5 = Window.partitionBy("customer_id", "plan")
w6 = Window.partitionBy("plan")
w7 = Window.partitionBy("customer_id", "status").orderBy("prev_monthly_fee", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

enriched = (
    df_subscriptions
    .select(
        "*",
        F.lag("monthly_fee", 1, 0).over(w).alias("prev_monthly_fee"),
        F.ntile(10).over(w2).alias("monthly_fee_bucket"),
    )
)

with_windows = (
    enriched
    .withColumn("next_monthly_fee_bucket", F.lead("monthly_fee_bucket", 2).over(w3))
    .withColumn("customer_id_avg_monthly_fee", F.avg("monthly_fee").over(w4))
    .withColumn("next_prev_monthly_fee", F.lead("prev_monthly_fee", 1, 0).over(w))
)

enriched_subscriptions = (
    with_windows
    .alias("a")
    .join(df_customers.alias("d"), F.col("a.customer_id") == F.col("d.id"), "inner")
    .select("a.*", F.col("d.segment"), F.col("d.signup_date"))
)

enriched_subscriptions_2 = (
    enriched_subscriptions
    .withColumn("customer_id_count_customer_id_avg_monthly_fee", F.count("customer_id_avg_monthly_fee").over(w5))
    .withColumn("monthly_fee_share", F.round((F.col("monthly_fee") / F.sum("monthly_fee").over(w6)), 4))
    .withColumn("running_next_monthly_fee_bucket", F.sum("next_monthly_fee_bucket").over(w7))
)

result = (
    enriched_subscriptions_2
    .groupBy("segment")
    .agg(
        F.size(F.collect_set("plan")).alias("num_plans"),
        F.sort_array(F.collect_set("segment")).alias("segment_list"),
        F.sum(F.when(F.col("plan") == "standard", F.col("monthly_fee_share")).otherwise(0)).alias("standard_monthly_fee_share"),
        F.sum(F.when(F.col("plan") == "basic", F.col("next_prev_monthly_fee")).otherwise(0)).alias("basic_next_prev_monthly_fee"),
    )
    .orderBy("num_plans")
)
