from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("paid_at") >= "2025-05-01")
    .groupBy(F.hour("paid_at").alias("paid_hour"), "method")
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("status"))).alias("status_values"),
        F.sum("amount").alias("total_amount"),
    )
    .filter(F.col("total_amount") > 4100)
)
