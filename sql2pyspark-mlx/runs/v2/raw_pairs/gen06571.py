from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy(F.col("end_date").desc(), "id")
w2 = Window.partitionBy("customer_id").orderBy("start_date", "id").rowsBetween(-6, Window.currentRow)

result = (
    df_subscriptions
    .select(
        "id",
        "customer_id",
        "end_date",
        "start_date",
        "monthly_fee",
        F.row_number().over(w).alias("row_num"),
        F.round(F.avg("monthly_fee").over(w2), 2).alias("monthly_fee_moving_avg_7"),
    )
)
