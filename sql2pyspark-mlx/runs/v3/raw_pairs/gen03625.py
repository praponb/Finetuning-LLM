from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy(F.col("monthly_fee").desc())
w2 = Window.partitionBy("plan").orderBy("end_date", "id").rowsBetween(-2, Window.currentRow)

result = (
    df_subscriptions
    .filter(F.col("customer_id").isin([7, 13, 15]))
    .select(
        "id",
        "customer_id",
        "monthly_fee",
        "plan",
        "end_date",
        F.rank().over(w).alias("monthly_fee_rank"),
        F.round(F.avg("monthly_fee").over(w2), 2).alias("monthly_fee_moving_avg_3"),
    )
)
