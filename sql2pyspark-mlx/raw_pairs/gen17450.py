from pyspark.sql import functions as F

result = (
    df_payments
    .groupBy(
        F.date_format("paid_at", "yyyy-MM").alias("paid_month"),
        "method",
        "status",
    )
    .agg(
        F.max("paid_at").alias("latest_paid_at"),
        F.count("*").alias("num_payments"),
        F.sort_array(F.collect_set("method")).alias("method_list"),
    )
)
