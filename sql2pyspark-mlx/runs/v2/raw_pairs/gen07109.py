from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("method").isNotNull())
    .groupBy(
        F.hour("paid_at").alias("paid_hour"),
        F.when(F.col("amount") >= 1690, "high").when(F.col("amount") >= 540, "mid").otherwise("low").alias("amount_bucket"),
    )
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("method"))).alias("method_values"),
        F.sum(F.when(F.col("status") == "REFUNDED", 1).otherwise(0)).alias("refunded_count"),
    )
    .filter(F.col("refunded_count") > 58)
    .orderBy(F.col("amount_bucket").desc(), "paid_hour")
    .limit(1)
)
