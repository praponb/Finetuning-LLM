from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id", "status").orderBy(F.col("monthly_fee").desc())
w_running = Window.partitionBy("customer_id")

result = (
    df_subscriptions
    .filter(F.col("status").isin(["EXPIRED", "CANCELLED", "PAUSED"]))
    .select(
        "id",
        "customer_id",
        "start_date",
        "end_date",
        "status",
        F.percent_rank().over(w).alias("monthly_fee_percent_rank"),
        F.avg("monthly_fee").over(w_running).alias("customer_id_avg_monthly_fee"),
    )
)
