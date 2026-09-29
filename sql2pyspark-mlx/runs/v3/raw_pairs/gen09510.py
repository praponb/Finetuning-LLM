from pyspark.sql import functions as F

result = (
    df_payments
    .filter((F.col("method") == "cash") | F.col("order_id").isin([5, 11, 26, 39]))
    .groupBy(
        F.month("paid_at").alias("paid_month"),
        F.when(F.col("method").isin(["cash", "promptpay"]), "core").otherwise("other").alias("method_group"),
    )
    .agg(
        F.sort_array(F.collect_set("method")).alias("method_list"),
        F.round(F.stddev("amount"), 2).alias("stddev_amount"),
        F.sum(F.when(F.col("method") == "card", 1).otherwise(0)).alias("card_count"),
    )
    .filter(F.col("card_count") > 35)
    .orderBy(F.col("stddev_amount").desc())
)
