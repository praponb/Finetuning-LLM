from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.month("end_date"), "status")
w2 = Window.partitionBy(F.year("end_date"), "customer_id").orderBy("end_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("status").orderBy("end_date", "id")
w4 = Window.orderBy("monthly_fee", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("plan").orderBy("end_date", "id").rowsBetween(-2, Window.currentRow)

scored = (
    df_subscriptions
    .select(
        "*",
        F.avg("monthly_fee").over(w).alias("end_date_month_avg_monthly_fee"),
        F.last("monthly_fee", ignorenulls=True).over(w2).alias("last_known_monthly_fee"),
        F.row_number().over(w3).alias("row_seq"),
        F.sum("monthly_fee").over(w4).alias("running_monthly_fee"),
        F.avg("monthly_fee").over(w5).alias("monthly_fee_avg_2"),
    )
)

result = (
    scored
    .groupBy(F.date_trunc("WEEK", "end_date").alias("week_start"))
    .agg(
        F.size(F.collect_set("plan")).alias("num_plans"),
        F.size(F.collect_set("plan")).alias("num_plans_2"),
        F.count_if(F.col("status") == "EXPIRED").alias("expired_events"),
        F.size(F.collect_set("status")).alias("num_status"),
        F.percentile_approx("end_date_month_avg_monthly_fee", [0.25, 0.5, 0.75]).alias("end_date_month_avg_monthly_fee_quartiles"),
        F.sum(F.when(F.col("status") == "ACTIVE", F.col("row_seq")).otherwise(0)).alias("active_row_seq"),
    )
    .orderBy(F.col("num_plans_2").desc())
)
