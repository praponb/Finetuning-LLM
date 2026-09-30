from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("paid_at") < "2024-01-01")
    .groupBy(F.to_date("paid_at").alias("paid_day"))
    .agg(F.sort_array(F.collect_set("method")).alias("method_list"))
)
