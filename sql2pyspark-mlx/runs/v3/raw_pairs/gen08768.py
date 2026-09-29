from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy("start_date", "id")

result = (
    df_subscriptions
    .filter(F.month("end_date").isin([1, 5, 10]))
    .select(
        "id",
        "status",
        "start_date",
        "monthly_fee",
        F.sum("monthly_fee").over(w).alias("running_monthly_fee"),
        F.lead("monthly_fee", 2).over(w).alias("next_monthly_fee"),
    )
)
