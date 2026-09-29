from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.month("end_date"), "plan")
w2 = Window.partitionBy(F.month("end_date"), "status")

result = (
    df_subscriptions
    .select(
        "id",
        "end_date",
        "plan",
        "monthly_fee",
        "status",
        F.round((F.col("monthly_fee") / F.sum("monthly_fee").over(w)), 3).alias("monthly_fee_share"),
        F.max("monthly_fee").over(w2).alias("end_date_month_max_monthly_fee"),
    )
)
