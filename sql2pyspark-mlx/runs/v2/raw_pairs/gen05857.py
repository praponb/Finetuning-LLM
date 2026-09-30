from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.hour("paid_at").between(12, 19) & (F.col("paid_at") >= "2024-10-01"))
    .groupBy("status", "method")
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("status"))).alias("status_values"),
        F.countDistinct("amount").alias("unique_statuss"),
    )
    .filter(F.col("unique_statuss") >= 3)
    .orderBy(F.col("status_values").desc())
)
