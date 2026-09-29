from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("plan", "status").orderBy("end_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("status").orderBy("end_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.orderBy(F.col("monthly_fee").desc_nulls_first())
w4 = Window.orderBy(F.col("monthly_fee").desc(), "id")

result = (
    df_subscriptions
    .select(
        "*",
        F.sum("monthly_fee").over(w).alias("cumulative_monthly_fee"),
        F.sum("monthly_fee").over(w2).alias("running_monthly_fee"),
        F.sum("monthly_fee").over(w2).alias("cumulative_monthly_fee_2"),
        F.rank().over(w3).alias("monthly_fee_rank"),
        F.ntile(4).over(w4).alias("monthly_fee_bucket"),
    )
)
