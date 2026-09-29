from pyspark.sql import functions as F

joined = (
    df_payments
    .alias("e")
    .join(df_orders.alias("o"), F.col("e.order_id") == F.col("o.id"), "left")
    .select("e.*", F.col("o.order_date"))
)

result = (
    joined
    .groupBy(
        "method",
        F.when(F.col("amount") >= 1510, "PREMIUM").otherwise("STANDARD").alias("amount_tier"),
    )
    .agg(
        F.max("paid_at").alias("ended_at"),
        F.count(F.when(F.col("method") == "bank_transfer", 1)).alias("bank_transfer_rows"),
        F.max("paid_at").alias("ended_at_2"),
        F.size(F.collect_set("method")).alias("num_methods"),
        F.count(F.when(F.col("method") == "paypal", 1)).alias("paypal_rows"),
        F.sort_array(F.collect_set("method")).alias("method_list"),
    )
    .orderBy(F.col("amount_tier").desc_nulls_last())
)
