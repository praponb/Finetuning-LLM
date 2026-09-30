from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.hour("paid_at").between(11, 15) & F.col("order_id").isNull())
    .groupBy("status")
    .agg(F.sort_array(F.collect_set("method")).alias("method_list"))
)
