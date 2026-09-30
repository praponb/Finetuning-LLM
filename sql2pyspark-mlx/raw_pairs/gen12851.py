from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy("start_date", "id").rowsBetween(-3, Window.currentRow)
w2 = Window.partitionBy("plan")

scored = (
    df_subscriptions
    .filter(F.col("customer_id") == 22)
    .select(
        "id",
        "customer_id",
        "start_date",
        "end_date",
        "status",
        F.min("monthly_fee").over(w).alias("last4_min_monthly_fee"),
        F.sum("monthly_fee").over(w).alias("last4_sum_monthly_fee"),
        F.avg("monthly_fee").over(w).alias("last4_avg_monthly_fee"),
        F.round((F.col("monthly_fee") / F.sum("monthly_fee").over(w2)), 4).alias("monthly_fee_share"),
    )
)

result = (
    scored
    .select(
        "id",
        "customer_id",
        "start_date",
        "status",
        "last4_avg_monthly_fee",
        "monthly_fee_share",
        F.when(F.col("status").rlike("^(ACTIVE|PAUSED)$"), "GROUP_A").when(F.col("status").rlike("^(CANCELLED|EXPIRED)$"), "GROUP_B").otherwise("EXTENDED").alias("status_zone"),
    )
    .orderBy(F.col("monthly_fee_share").asc_nulls_last())
)
