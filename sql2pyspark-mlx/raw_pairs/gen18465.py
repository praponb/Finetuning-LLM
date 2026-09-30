from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("plan", "customer_id").orderBy("monthly_fee", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_ordered = Window.partitionBy("status").orderBy(F.col("monthly_fee").desc_nulls_first())
w_recent = Window.partitionBy(F.year("start_date"), "status").orderBy("end_date", "id").rowsBetween(-6, -1)

result = (
    df_subscriptions
    .filter(F.year("start_date") >= 2024)
    .select(
        "id",
        "start_date",
        "end_date",
        F.first("monthly_fee", ignorenulls=True).over(w).alias("first_known_monthly_fee"),
        F.rank().over(w_ordered).alias("monthly_fee_rank"),
        F.avg("monthly_fee").over(w_recent).alias("monthly_fee_avg_6_prior"),
    )
)
