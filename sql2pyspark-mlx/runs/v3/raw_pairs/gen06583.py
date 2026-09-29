from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("order_id").isin([5, 13, 15, 36]))
    .groupBy("status", "method")
    .agg(
        F.count("amount").alias("amount_count"),
        F.sort_array(F.collect_set("method")).alias("method_list"),
    )
    .filter(F.col("amount_count") > 9)
    .orderBy("status")
)
