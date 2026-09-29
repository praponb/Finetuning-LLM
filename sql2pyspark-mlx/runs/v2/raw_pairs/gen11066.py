from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id")

result = (
    df_subscriptions
    .filter(F.col("plan") == "standard")
    .select(
        "id",
        "customer_id",
        "monthly_fee",
        F.sum("monthly_fee").over(w).alias("customer_id_sum_monthly_fee"),
    )
)
