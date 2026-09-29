from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy("start_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.orderBy(F.col("end_date").desc())

result = (
    df_subscriptions
    .select(
        "id",
        "customer_id",
        "start_date",
        "monthly_fee",
        "end_date",
        F.sum("monthly_fee").over(w).alias("running_monthly_fee"),
        F.percent_rank().over(w2).alias("end_date_pct_rank"),
    )
)
