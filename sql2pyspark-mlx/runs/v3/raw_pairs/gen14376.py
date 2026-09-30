from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("plan").orderBy("end_date", "id")
w2 = Window.partitionBy("customer_id").orderBy("start_date", "id")

result = (
    df_subscriptions
    .select(
        "id",
        "plan",
        "end_date",
        "monthly_fee",
        "customer_id",
        "start_date",
        F.lag("monthly_fee", 1).over(w).alias("prev_monthly_fee"),
        F.row_number().over(w2).alias("rn"),
    )
    .orderBy(F.col("customer_id").desc())
)
