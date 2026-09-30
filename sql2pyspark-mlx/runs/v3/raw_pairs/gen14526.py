from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("end_date").desc(), "id")
w2 = Window.partitionBy("customer_id").orderBy("start_date", "id")

result = (
    df_subscriptions
    .filter(F.col("monthly_fee") <= 180)
    .select(
        "id",
        "end_date",
        "customer_id",
        "start_date",
        "monthly_fee",
        F.ntile(3).over(w).alias("end_date_ntile"),
        (F.col("monthly_fee") - F.lag("monthly_fee", 2).over(w2)).alias("monthly_fee_change"),
    )
)
