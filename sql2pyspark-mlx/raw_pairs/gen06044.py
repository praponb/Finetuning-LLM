from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy("monthly_fee", "id")
w2 = Window.partitionBy("status", "plan").orderBy("monthly_fee", "id")

result = (
    df_subscriptions
    .filter(F.col("start_date") < "2025-04-01")
    .select(
        "id",
        "plan",
        "start_date",
        "end_date",
        "monthly_fee",
        "status",
        F.ntile(3).over(w).alias("monthly_fee_bucket"),
        F.ntile(4).over(w2).alias("monthly_fee_bucket_2"),
    )
)
