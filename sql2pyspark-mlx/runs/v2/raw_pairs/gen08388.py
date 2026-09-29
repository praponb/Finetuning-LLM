from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("status") == "REFUNDED")
    .groupBy(F.hour("paid_at").alias("paid_hour"))
    .agg(
        (F.max("amount") - F.min("amount")).alias("amount_range"),
        F.sum(F.when(F.col("status") == "SUCCESS", F.col("amount")).otherwise(0)).alias("success_amount"),
        F.concat_ws(", ", F.sort_array(F.collect_set("status"))).alias("status_values"),
    )
    .filter(F.col("success_amount") > 44)
)
