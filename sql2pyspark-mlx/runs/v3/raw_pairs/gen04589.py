from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy("end_date", "id")
w2 = Window.partitionBy("plan").orderBy("start_date", "id")

result = (
    df_subscriptions
    .select(
        "id",
        "status",
        "end_date",
        "monthly_fee",
        "plan",
        "start_date",
        F.first("monthly_fee").over(w).alias("first_monthly_fee"),
        F.lag("monthly_fee", 2).over(w2).alias("prev_monthly_fee"),
    )
    .orderBy(F.col("monthly_fee").desc())
)
