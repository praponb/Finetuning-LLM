from pyspark.sql import functions as F

result = (
    df_payments
    .filter((F.hour("paid_at").between(6, 9) & F.col("amount").between(650, 1250)) | F.col("order_id").isNotNull())
    .groupBy("method")
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("status"))).alias("status_values"),
    )
)
