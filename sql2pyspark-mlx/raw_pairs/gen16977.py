from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy("monthly_fee", "id")
w2 = Window.partitionBy(F.year("end_date"), "customer_id")
w_recent = Window.partitionBy(F.month("end_date")).orderBy(F.col("monthly_fee").desc(), "id")

cleaned = (
    df_subscriptions
    .filter(F.col("customer_id").isNotNull() & (F.col("monthly_fee") >= 90))
    .select(
        "id",
        "customer_id",
        "plan",
        "start_date",
        "end_date",
        "monthly_fee",
        F.upper("plan").alias("plan_upper"),
    )
)

result = (
    cleaned
    .filter(F.col("customer_id").isNull())
    .select(
        "id",
        "customer_id",
        F.lag("monthly_fee", 1).over(w).alias("prev_monthly_fee"),
        F.round((F.col("monthly_fee") / F.sum("monthly_fee").over(w2)), 4).alias("monthly_fee_share"),
        F.ntile(5).over(w_recent).alias("monthly_fee_bucket"),
    )
)
