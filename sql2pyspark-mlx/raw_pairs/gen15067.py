from pyspark.sql import functions as F

result = (
    df_payments
    .groupBy("status", F.date_format("paid_at", "yyyy-MM").alias("paid_month"))
    .agg(F.sort_array(F.collect_set("status")).alias("status_list"))
    .orderBy(F.col("paid_month").desc())
)
