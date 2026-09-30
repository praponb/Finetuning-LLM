from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy(F.col("end_date").desc(), "id")
w2 = Window.partitionBy("status").orderBy("end_date", "id")

result = (
    df_subscriptions
    .filter(F.col("monthly_fee") >= 240)
    .select(
        "id",
        "status",
        "end_date",
        "monthly_fee",
        F.row_number().over(w).alias("end_date_row"),
        F.lag("monthly_fee", 2, 0).over(w2).alias("prev_monthly_fee"),
    )
)
