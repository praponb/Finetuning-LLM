from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_ordered = Window.partitionBy("status", "customer_id").orderBy("monthly_fee", "id").rowsBetween(-2, Window.currentRow)

scored = (
    df_subscriptions
    .filter(F.col("end_date") > "2025-02-01")
    .select(
        "id",
        "customer_id",
        "status",
        F.max("monthly_fee").over(w_ordered).alias("last3_max_monthly_fee"),
        F.min("monthly_fee").over(w_ordered).alias("last3_min_monthly_fee"),
        F.avg("monthly_fee").over(w_ordered).alias("last3_avg_monthly_fee"),
        F.count("monthly_fee").over(w_ordered).alias("last3_count_monthly_fee"),
    )
)

result = (
    scored
    .select(
        "id",
        "status",
        "last3_max_monthly_fee",
        "last3_min_monthly_fee",
        "last3_avg_monthly_fee",
        "last3_count_monthly_fee",
        F.when(F.col("status").rlike("^(PAUSED|ACTIVE)$"), "CORE").otherwise("GROUP_B").alias("status_zone"),
    )
    .orderBy("last3_avg_monthly_fee")
)
