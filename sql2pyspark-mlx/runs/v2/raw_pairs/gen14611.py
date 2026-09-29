from pyspark.sql import functions as F

result = (
    df_payments
    .groupBy("method", F.hour("paid_at").alias("paid_hour"))
    .agg(
        F.round(F.stddev("amount"), 2).alias("stddev_amount"),
        F.sort_array(F.collect_set("status")).alias("status_list"),
    )
    .filter(F.col("stddev_amount") > 73)
    .orderBy("paid_hour")
)
