from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("plan")
w2 = Window.partitionBy("plan").orderBy("monthly_fee", "id")
w3 = Window.orderBy("end_date", "id").rowsBetween(-2, -1)
w4 = Window.partitionBy("status").orderBy("end_date", "id")

enriched = (
    df_subscriptions
    .select(
        "id",
        "end_date",
        "status",
        F.round((F.col("monthly_fee") / F.sum("monthly_fee").over(w)), 4).alias("monthly_fee_share"),
        F.ntile(5).over(w2).alias("monthly_fee_bucket"),
        F.avg("monthly_fee").over(w3).alias("monthly_fee_avg_2_prior"),
        F.lag("monthly_fee", 1).over(w4).alias("prev_monthly_fee"),
    )
)

result = (
    enriched
    .select(
        "id",
        "end_date",
        "monthly_fee_share",
        "monthly_fee_bucket",
        "monthly_fee_avg_2_prior",
        "prev_monthly_fee",
    )
    .orderBy(F.col("monthly_fee_bucket").desc())
)
