from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("start_date", "id")

result = (
    df_subscriptions
    .select(
        "id",
        "start_date",
        "monthly_fee",
        F.lead("monthly_fee", 2).over(w).alias("next_monthly_fee"),
    )
)
