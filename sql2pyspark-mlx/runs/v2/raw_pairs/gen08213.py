from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("plan").orderBy(F.col("monthly_fee").desc())
w2 = Window.orderBy("start_date", "id")

result = (
    df_subscriptions
    .select(
        "id",
        "plan",
        "monthly_fee",
        "start_date",
        F.rank().over(w).alias("rnk"),
        F.lag("monthly_fee", 1, 0).over(w2).alias("prev_monthly_fee"),
    )
)
