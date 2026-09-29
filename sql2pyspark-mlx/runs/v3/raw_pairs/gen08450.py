from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id")

result = (
    df_subscriptions
    .select(
        "id",
        "customer_id",
        "monthly_fee",
        F.max("monthly_fee").over(w).alias("customer_id_max_monthly_fee"),
    )
)
