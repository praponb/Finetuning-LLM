from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy("start_date_month_end", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("status", "customer_id").orderBy(F.col("monthly_fee").desc(), "id")
w3 = Window.partitionBy("status").orderBy("monthly_fee", "id")
w4 = Window.partitionBy("status").orderBy(F.col("monthly_fee").desc(), "id")
w5 = Window.partitionBy("status", "customer_id").orderBy("start_date", "id").rowsBetween(-2, Window.currentRow)

cleaned = (
    df_subscriptions
    .filter(F.col("end_date") >= F.to_date(F.lit("2023-12-19")))
    .select(
        "id",
        "customer_id",
        "start_date",
        "monthly_fee",
        "status",
        F.last_day("start_date").alias("start_date_month_end"),
    )
)

result = (
    cleaned
    .withColumn("last_known_monthly_fee", F.last("monthly_fee", ignorenulls=True).over(w))
    .withColumn("monthly_fee_bucket", F.ntile(10).over(w2))
    .withColumn("monthly_fee_bucket_2", F.ntile(5).over(w3))
    .withColumn("monthly_fee_bucket_3", F.ntile(5).over(w4))
    .withColumn("monthly_fee_avg_2", F.avg("monthly_fee").over(w5))
)
