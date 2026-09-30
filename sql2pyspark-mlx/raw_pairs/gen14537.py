from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy("end_date", "id").rowsBetween(-2, -1)
w2 = Window.partitionBy(F.date_trunc("month", "start_date"), "customer_id").orderBy("start_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("status").orderBy(F.col("monthly_fee").desc(), "id")
w4 = Window.partitionBy("plan").orderBy("monthly_fee", "id")

scored = (
    df_subscriptions
    .select(
        "id",
        "start_date",
        "monthly_fee",
        "status",
        F.avg("monthly_fee").over(w).alias("monthly_fee_avg_2_prior"),
        F.sum("monthly_fee").over(w2).alias("running_monthly_fee"),
        F.ntile(5).over(w3).alias("monthly_fee_bucket"),
        F.ntile(3).over(w4).alias("monthly_fee_bucket_2"),
    )
)

result = (
    scored
    .select(
        "id",
        "start_date",
        "monthly_fee",
        "status",
        "monthly_fee_avg_2_prior",
        "running_monthly_fee",
        "monthly_fee_bucket_2",
    )
    .orderBy(F.col("monthly_fee_avg_2_prior").desc(), "monthly_fee_bucket_2")
)
