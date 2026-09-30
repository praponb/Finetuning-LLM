from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("plan").orderBy("start_date", "id")

result = (
    df_subscriptions
    .filter(F.col("monthly_fee").between(90, 120))
    .select(
        "id",
        "plan",
        "start_date",
        "end_date",
        F.lead("monthly_fee").over(w).alias("next_monthly_fee"),
        F.lag("monthly_fee").over(w).alias("prev_monthly_fee"),
        F.row_number().over(w).alias("event_seq"),
    )
)
