from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("end_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_grp = Window.partitionBy("status", "customer_id").orderBy("start_date", "id")
w3 = Window.orderBy(F.col("monthly_fee").desc(), "id")
w4 = Window.partitionBy("status").orderBy("monthly_fee", "id")
w5 = Window.orderBy("end_date_period", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

base = (
    df_subscriptions
    .filter(F.col("customer_id").isNotNull())
    .select(
        "id",
        "customer_id",
        "start_date",
        "end_date",
        "monthly_fee",
        "status",
        F.date_trunc("month", "end_date").alias("end_date_period"),
    )
)

result = (
    base
    .filter(F.col("monthly_fee") <= 130)
    .select(
        "*",
        F.sum("monthly_fee").over(w).alias("cumulative_monthly_fee"),
        F.row_number().over(w_grp).alias("row_seq"),
        F.ntile(10).over(w3).alias("monthly_fee_bucket"),
        F.lag("monthly_fee", 1).over(w4).alias("prev_monthly_fee"),
        F.sum("monthly_fee").over(w5).alias("running_monthly_fee"),
    )
)
