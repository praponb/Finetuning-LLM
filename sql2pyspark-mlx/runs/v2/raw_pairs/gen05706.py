from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.col("customer_id").isNotNull())
    .groupBy("status")
    .agg(F.sort_array(F.collect_set("status")).alias("status_list"))
)
