from pyspark.sql import functions as F

src = (
    df_payments
    .filter(F.col("amount").between(510, 1070))
    .withColumn("method_status_key", F.concat_ws("-", "method", "status"))
)

order_id_summary = (
    src
    .groupBy("order_id")
    .agg(
        F.sort_array(F.collect_set("method")).alias("method_list"),
        F.min("paid_at").alias("first_paid_at"),
    )
)

result = (
    order_id_summary
    .select("order_id", "method_list", "first_paid_at")
    .orderBy("order_id")
    .limit(1)
)
