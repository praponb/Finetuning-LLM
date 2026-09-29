from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("order_id") == 14)
    .groupBy("status")
    .agg(
        F.count("*").alias("num_payments"),
        F.sort_array(F.collect_set("status")).alias("status_list"),
    )
    .filter(F.col("num_payments") < 3)
    .orderBy(F.col("num_payments").desc())
)
