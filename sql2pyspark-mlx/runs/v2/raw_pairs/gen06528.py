from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy("end_date", "id")

result = (
    df_subscriptions
    .filter(F.col("status").isin(["ACTIVE", "EXPIRED", "CANCELLED"]))
    .select(
        "id",
        "customer_id",
        "end_date",
        "monthly_fee",
        F.lag("monthly_fee", 1).over(w).alias("prev_monthly_fee"),
    )
)
