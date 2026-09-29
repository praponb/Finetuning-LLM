from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id")
w2 = Window.partitionBy("status").orderBy("monthly_fee", "id")

result = (
    df_subscriptions
    .select(
        "id",
        "customer_id",
        "monthly_fee",
        "status",
        F.min("monthly_fee").over(w).alias("customer_id_min_monthly_fee"),
        F.row_number().over(w2).alias("row_num"),
    )
)
