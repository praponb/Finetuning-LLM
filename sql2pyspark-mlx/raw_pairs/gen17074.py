from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.hour("paid_at").between(11, 15) & (F.col("amount") < 880))
    .groupBy("status")
    .agg(
        F.count(F.when(F.col("method") == "promptpay", 1)).alias("promptpay_count"),
        F.concat_ws(", ", F.sort_array(F.collect_set("method"))).alias("method_values"),
    )
    .orderBy(F.col("status").desc())
)
