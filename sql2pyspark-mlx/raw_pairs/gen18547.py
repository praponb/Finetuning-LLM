from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("start_date", "id").rowsBetween(-5, Window.currentRow)
w2 = Window.partitionBy("plan").orderBy("end_date", "id")

result = (
    df_subscriptions
    .filter(F.col("monthly_fee") < 120)
    .select(
        "id",
        "customer_id",
        "plan",
        "start_date",
        "monthly_fee",
        F.avg("monthly_fee").over(w).alias("monthly_fee_avg_5"),
        F.row_number().over(w2).alias("row_seq"),
    )
)
