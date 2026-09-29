from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("method") == "promptpay")
    .groupBy(F.year("paid_at").alias("paid_year"))
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("method"))).alias("method_values"),
        F.sum(F.when(F.col("status") == "SUCCESS", 1).otherwise(0)).alias("success_count"),
        (F.max("amount") - F.min("amount")).alias("amount_range"),
    )
    .orderBy("paid_year")
)
