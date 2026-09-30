from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status")
w2 = Window.partitionBy("status").orderBy("monthly_fee", "id")

result = (
    df_subscriptions
    .select(
        "id",
        "end_date",
        "monthly_fee",
        "status",
        F.round((F.col("monthly_fee") / F.sum("monthly_fee").over(w)), 4).alias("monthly_fee_share"),
        F.lag("monthly_fee", 1).over(w2).alias("prev_monthly_fee"),
    )
)
