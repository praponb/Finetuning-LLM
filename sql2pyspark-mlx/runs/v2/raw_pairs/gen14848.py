from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy("start_date", "id")
w2 = Window.partitionBy("customer_id").orderBy("monthly_fee", "id")

result = (
    df_subscriptions
    .select(
        "id",
        "customer_id",
        "start_date",
        "monthly_fee",
        F.lead("monthly_fee", 1, 0).over(w).alias("next_monthly_fee"),
        F.row_number().over(w2).alias("row_num"),
    )
)
