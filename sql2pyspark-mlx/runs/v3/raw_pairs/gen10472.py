from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id", "status")
w2 = Window.partitionBy("customer_id")

result = (
    df_subscriptions
    .filter(F.year("end_date") >= 2024)
    .select(
        "id",
        "customer_id",
        "status",
        "monthly_fee",
        F.round((F.col("monthly_fee") / F.sum("monthly_fee").over(w)), 3).alias("monthly_fee_share"),
        F.max("monthly_fee").over(w2).alias("customer_id_max_monthly_fee"),
    )
)
