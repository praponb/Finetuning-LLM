from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("plan").orderBy(F.col("monthly_fee").desc(), "id")
w2 = Window.partitionBy("status", "customer_id")

result = (
    df_subscriptions
    .select(
        "id",
        "plan",
        "monthly_fee",
        "status",
        "customer_id",
        F.row_number().over(w).alias("monthly_fee_row"),
        F.sum("monthly_fee").over(w2).alias("status_sum_monthly_fee"),
    )
)
