from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("start_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy(F.year("end_date")).orderBy("monthly_fee")

result = (
    df_subscriptions
    .filter(F.year("start_date") == 2023)
    .select(
        "id",
        "customer_id",
        "end_date",
        "monthly_fee",
        "status",
        F.sum("monthly_fee").over(w).alias("running_monthly_fee"),
        F.dense_rank().over(w2).alias("monthly_fee_dense_rank"),
    )
)
