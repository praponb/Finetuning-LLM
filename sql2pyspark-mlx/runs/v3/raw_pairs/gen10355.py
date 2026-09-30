from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("plan").orderBy("end_date", "id")
w2 = Window.partitionBy("status").orderBy("monthly_fee")

result = (
    df_subscriptions
    .select(
        "id",
        "plan",
        "end_date",
        "monthly_fee",
        "status",
        (F.col("monthly_fee") - F.lag("monthly_fee", 1).over(w)).alias("monthly_fee_change"),
        F.rank().over(w2).alias("rnk"),
    )
    .orderBy("monthly_fee", "status")
)
