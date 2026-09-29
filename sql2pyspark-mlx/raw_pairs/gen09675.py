from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy("end_date", "id")

result = (
    df_subscriptions
    .filter(F.col("customer_id").isNull())
    .select(
        "id",
        "customer_id",
        "end_date",
        "monthly_fee",
        F.lead("monthly_fee", 2, 0).over(w).alias("next_monthly_fee"),
    )
    .orderBy("end_date")
)
