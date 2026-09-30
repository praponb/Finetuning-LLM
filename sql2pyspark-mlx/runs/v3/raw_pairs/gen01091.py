from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("end_date", "id")

result = (
    df_subscriptions
    .filter(F.year("end_date") >= 2024)
    .select(
        "id",
        "end_date",
        "monthly_fee",
        F.lead("monthly_fee", 1).over(w).alias("next_monthly_fee"),
    )
    .orderBy(F.col("end_date").desc())
)
