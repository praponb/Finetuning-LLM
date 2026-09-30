from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_recent = Window.partitionBy("customer_id").orderBy("end_date", "id").rowsBetween(-6, Window.currentRow)

scored = (
    df_subscriptions
    .filter(F.col("start_date") < "2025-06-09")
    .withColumn("last7_avg_monthly_fee", F.avg("monthly_fee").over(w_recent))
    .withColumn("last7_count_monthly_fee", F.count("monthly_fee").over(w_recent))
    .withColumn("last7_max_monthly_fee", F.max("monthly_fee").over(w_recent))
)

result = (
    scored
    .withColumn("quarter_start", F.date_trunc("QUARTER", "start_date"))
    .groupingSets(
        [["customer_id", "quarter_start"], ["customer_id"]],
        "customer_id",
        "quarter_start",
    )
    .agg(
        F.sum(F.when(F.col("status") == "PAUSED", F.col("last7_avg_monthly_fee")).otherwise(0)).alias("paused_last7_avg_monthly_fee"),
        F.percentile_approx("last7_max_monthly_fee", [0.25, 0.5, 0.75]).alias("last7_max_monthly_fee_quartiles"),
        F.grouping_id().alias("grouping_id"),
    )
    .filter(F.col("paused_last7_avg_monthly_fee") > 0)
    .orderBy(F.col("quarter_start").asc_nulls_first())
)
