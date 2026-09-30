from pyspark.sql import functions as F

result = (
    df_payments
    .groupBy(F.month("paid_at").alias("paid_month"))
    .agg(F.sort_array(F.collect_set("method")).alias("method_list"))
)
