from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_recent = Window.partitionBy("status").orderBy("monthly_fee", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy(F.year("start_date"), "customer_id")
w3 = Window.partitionBy("plan", "customer_id").orderBy("start_date", "id")

result = (
    df_subscriptions
    .select(
        "id",
        "customer_id",
        "plan",
        "start_date",
        "end_date",
        "monthly_fee",
        "status",
        F.sum("monthly_fee").over(w_recent).alias("running_monthly_fee"),
        F.round((F.col("monthly_fee") / F.sum("monthly_fee").over(w2)), 4).alias("monthly_fee_share"),
        F.lag("monthly_fee", 2, 0).over(w3).alias("prev_monthly_fee"),
    )
)
