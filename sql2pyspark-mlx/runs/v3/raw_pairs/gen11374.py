from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("order_id").isin([12, 22, 34, 36]))
    .groupBy(F.to_date("paid_at").alias("paid_day"), "method")
    .agg(
        F.sort_array(F.collect_set("method")).alias("method_list"),
        F.count("amount").alias("amount_count"),
    )
    .filter(F.col("amount_count") >= 4)
)
