from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy("end_date", "id")

result = (
    df_subscriptions
    .filter(~F.col("status").isin(["EXPIRED", "PAUSED", "ACTIVE"]))
    .select(
        "id",
        "customer_id",
        "end_date",
        "monthly_fee",
        F.lead("monthly_fee", 2).over(w).alias("next_monthly_fee"),
    )
    .orderBy(F.col("next_monthly_fee").desc())
)
