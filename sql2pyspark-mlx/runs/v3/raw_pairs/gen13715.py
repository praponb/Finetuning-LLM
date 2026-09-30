from pyspark.sql import functions as F

result = (
    df_payments
    .agg(
        F.sum(F.when(F.col("method") == "paypal", F.col("amount")).otherwise(0)).alias("paypal_amount"),
        F.round(((100 * F.sum(F.when(F.col("status") == "REFUNDED", 1).otherwise(0))) / F.count("*")), 1).alias("pct_refunded"),
        F.sort_array(F.collect_set("method")).alias("method_list"),
    )
)
