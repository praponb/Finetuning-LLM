from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status", "customer_id").orderBy("monthly_fee", "id").rowsBetween(-5, -1)
w2 = Window.partitionBy("plan").orderBy("monthly_fee", "id")

with_windows = (
    df_subscriptions
    .withColumn("monthly_fee_avg_5_prior", F.avg("monthly_fee").over(w))
    .withColumn("row_seq", F.row_number().over(w2))
)

result = (
    with_windows
    .withColumn("monthly_fee_tier", F.when(F.col("monthly_fee") >= 206, "HIGH").otherwise("LOW"))
    .withColumn("activity_month", F.date_trunc("MONTH", "end_date"))
    .groupingSets(
        [["monthly_fee_tier", "activity_month"], ["monthly_fee_tier"], []],
        "monthly_fee_tier",
        "activity_month",
    )
    .agg(
        F.percentile_approx("row_seq", [0.25, 0.5, 0.75]).alias("row_seq_quartiles"),
        F.sum(F.when(F.col("plan") == "premium", F.col("row_seq")).otherwise(0)).alias("premium_row_seq"),
        F.min("monthly_fee").alias("min_monthly_fee"),
        F.size(F.collect_set("plan")).alias("num_plans"),
        F.count(F.when(F.col("plan") == "standard", 1)).alias("standard_count"),
        F.round(F.avg("monthly_fee_avg_5_prior"), 2).alias("avg_monthly_fee_avg_5_prior"),
    )
)
