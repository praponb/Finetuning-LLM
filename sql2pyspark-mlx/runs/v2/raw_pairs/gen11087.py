from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("plan").orderBy("start_date", "id")

result = (
    df_subscriptions
    .select(
        "id",
        "plan",
        "start_date",
        "monthly_fee",
        F.lead("monthly_fee", 1).over(w).alias("next_monthly_fee"),
    )
)
