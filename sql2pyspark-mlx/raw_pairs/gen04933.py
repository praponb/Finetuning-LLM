from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("plan", "customer_id").orderBy("end_date", "id")
w2 = Window.partitionBy("status").orderBy("start_date", "id").rowsBetween(-5, -1)
w3 = Window.partitionBy("customer_id")

result = (
    df_subscriptions
    .filter(F.col("monthly_fee") >= 140)
    .select(
        "id",
        "customer_id",
        "start_date",
        "end_date",
        "status",
        F.row_number().over(w).alias("event_seq"),
        F.avg("monthly_fee").over(w2).alias("monthly_fee_avg_5_prior"),
        F.max("monthly_fee").over(w3).alias("customer_id_max_monthly_fee"),
    )
)
