from pyspark.sql import functions as F

result = (
    df_payments
    .filter((F.col("paid_at") < "2023-03-01") & (F.to_date("paid_at") >= "2023-12-01"))
    .withColumn("paid_at_label", F.date_format("paid_at", "MMM yyyy"))
    .orderBy(F.col("order_id").desc(), "id")
    .limit(1)
)
