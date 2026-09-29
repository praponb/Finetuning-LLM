from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_part = Window.partitionBy("plan").orderBy("end_date", "id")
w2 = Window.orderBy("monthly_fee", "id").rowsBetween(-3, Window.currentRow)

result = (
    df_subscriptions
    .select(
        "*",
        F.row_number().over(w_part).alias("seq_no"),
        F.avg("monthly_fee").over(w2).alias("monthly_fee_avg_3"),
    )
)
