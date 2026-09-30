from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status", "customer_id").orderBy("end_date", "id")

result = (
    df_subscriptions
    .filter(F.col("start_date") <= "2025-07-01")
    .select(
        "id",
        "status",
        "customer_id",
        "end_date",
        "monthly_fee",
        F.lead("monthly_fee", 1).over(w).alias("next_monthly_fee"),
    )
    .orderBy(F.col("monthly_fee").desc(), F.col("end_date").desc())
)
