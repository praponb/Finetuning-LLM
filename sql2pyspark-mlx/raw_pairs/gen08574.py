from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status", "customer_id").orderBy("end_date", "id")
w2 = Window.partitionBy("plan").orderBy(F.col("monthly_fee").asc_nulls_last())
w3 = Window.orderBy("end_date", "id").rowsBetween(-6, -1)
w4 = Window.partitionBy("status").orderBy("monthly_fee", "id")
w5 = Window.partitionBy("status")

result = (
    df_subscriptions
    .select(
        "id",
        "plan",
        "monthly_fee",
        F.lag("monthly_fee", 1).over(w).alias("prev_monthly_fee"),
        F.dense_rank().over(w2).alias("monthly_fee_dense_rank"),
        F.avg("monthly_fee").over(w3).alias("monthly_fee_avg_6_prior"),
        F.lead("monthly_fee", 1).over(w4).alias("next_monthly_fee"),
        F.avg("monthly_fee").over(w5).alias("status_avg_monthly_fee"),
    )
)
