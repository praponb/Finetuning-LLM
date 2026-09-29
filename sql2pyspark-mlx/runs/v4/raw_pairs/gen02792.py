from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id", "status")
w_hist = Window.partitionBy("plan").orderBy("monthly_fee")
w_grp = Window.partitionBy("plan").orderBy("monthly_fee", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

metrics = (
    df_subscriptions
    .select(
        "*",
        F.avg("monthly_fee").over(w).alias("customer_id_avg_monthly_fee"),
        F.cume_dist().over(w_hist).alias("monthly_fee_cume_dist"),
        F.last("monthly_fee", ignorenulls=True).over(w_grp).alias("last_known_monthly_fee"),
    )
)

result = (
    metrics
    .select(
        "id",
        "plan",
        "start_date",
        "end_date",
        "monthly_fee",
        "status",
        "customer_id_avg_monthly_fee",
        "monthly_fee_cume_dist",
        "last_known_monthly_fee",
        F.when(F.col("status").rlike("^(EXPIRED|ACTIVE)$"), "CORE").when(F.col("status").rlike("^(CANCELLED|PAUSED)$"), "PRIORITY").otherwise("OTHER_ZONE").alias("status_zone"),
    )
    .orderBy(F.col("plan").desc_nulls_last())
)
