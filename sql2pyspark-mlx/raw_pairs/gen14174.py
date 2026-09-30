from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("plan")
w_hist = Window.partitionBy("status", "customer_id").orderBy("start_date_plus_months", "id")

filtered = (
    df_subscriptions
    .filter((F.col("plan") == "premium") & F.col("customer_id").isNotNull())
    .select(
        "id",
        "customer_id",
        "plan",
        "start_date",
        "monthly_fee",
        "status",
        F.add_months("start_date", 6).alias("start_date_plus_months"),
    )
)

result = (
    filtered
    .withColumn("monthly_fee_share", F.round((F.col("monthly_fee") / F.sum("monthly_fee").over(w)), 4))
    .withColumn("prev_monthly_fee", F.lag("monthly_fee", 2, 0).over(w_hist))
)
