from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy(F.col("monthly_fee").asc_nulls_first())
w_recent = Window.partitionBy("customer_id").orderBy("start_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("customer_id").orderBy("start_date", "id").rowsBetween(-6, Window.currentRow)

cleaned = (
    df_subscriptions
    .filter(F.month("end_date").isin([2, 7, 11]))
    .withColumn("status_prefix", F.substring("status", 1, 3))
)

result = (
    cleaned
    .withColumn("monthly_fee_rank", F.rank().over(w))
    .withColumn("cumulative_monthly_fee", F.sum("monthly_fee").over(w_recent))
    .withColumn("monthly_fee_avg_6", F.avg("monthly_fee").over(w3))
)
