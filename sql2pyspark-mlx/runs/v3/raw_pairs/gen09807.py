from pyspark.sql import functions as F

recent_payments = (
    df_payments
    .filter(~((F.col("method").isin(["cash", "card"]) & (F.col("order_id") == 10)) | F.col("order_id").isNotNull()))
)

status_summary = (
    recent_payments
    .groupBy("status")
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("method"))).alias("method_values"),
    )
)

result = (
    status_summary
    .orderBy(F.col("status").desc())
    .limit(10)
)
