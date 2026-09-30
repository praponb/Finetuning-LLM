from pyspark.sql import functions as F

valid_payments = (
    df_payments
    .filter((F.col("order_id").isNull() & F.col("method").isin(["cash", "promptpay"])) | (F.col("status") != "REFUNDED"))
)

payments_stats = (
    valid_payments
    .groupBy("order_id")
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("method"))).alias("method_values"),
        F.countDistinct("amount").alias("unique_methods"),
    )
)

result = (
    payments_stats
    .filter(F.col("unique_methods") < 4)
    .orderBy(F.col("order_id").desc())
    .limit(10)
)
