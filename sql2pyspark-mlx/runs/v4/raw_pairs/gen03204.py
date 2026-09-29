from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("plan").orderBy("end_date", "id")

result = (
    df_subscriptions
    .filter(F.col("monthly_fee").between(120, 220))
    .select(
        "id",
        "plan",
        "end_date",
        "monthly_fee",
        F.lag("monthly_fee", 1).over(w).alias("prev_monthly_fee"),
    )
)
