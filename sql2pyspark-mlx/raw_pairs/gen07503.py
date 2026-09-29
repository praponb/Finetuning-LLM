from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_ordered = Window.partitionBy("plan").orderBy("monthly_fee")
w2 = Window.partitionBy("customer_id")
w3 = Window.orderBy("end_date", "id").rowsBetween(-6, Window.currentRow)

metrics = (
    df_subscriptions
    .select(
        "id",
        "plan",
        "monthly_fee",
        "status",
        F.percent_rank().over(w_ordered).alias("monthly_fee_percent_rank"),
        F.avg("monthly_fee").over(w2).alias("customer_id_avg_monthly_fee"),
        F.avg("monthly_fee").over(w3).alias("monthly_fee_avg_6"),
    )
)

result = (
    metrics
    .withColumn("monthly_fee_tier", F.when(F.col("monthly_fee") >= 213, "PREMIUM").otherwise("STANDARD"))
    .withColumn("monthly_fee_percent_rank_tier", F.when(F.col("monthly_fee_percent_rank") >= 0.78, "HIGH").otherwise("LOW"))
    .groupingSets(
        [["monthly_fee_tier", "monthly_fee_percent_rank_tier"], ["monthly_fee_tier"]],
        "monthly_fee_tier",
        "monthly_fee_percent_rank_tier",
    )
    .agg(
        F.avg(F.when(F.col("plan") == "basic", F.col("monthly_fee_percent_rank"))).alias("basic_avg_monthly_fee_percent_rank"),
        F.size(F.collect_set("status")).alias("num_status"),
        F.sort_array(F.collect_set("status")).alias("status_list"),
        F.size(F.collect_set("status")).alias("num_status_2"),
    )
    .orderBy(F.col("monthly_fee_percent_rank_tier").asc_nulls_first())
)
