from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("status").isin(["SUCCESS", "FAILED"]))
    .groupBy(F.quarter("paid_at").alias("paid_quarter"), "method")
    .agg(
        (F.max("amount") - F.min("amount")).alias("amount_range"),
        F.concat_ws(", ", F.sort_array(F.collect_set("method"))).alias("method_values"),
    )
    .filter(F.col("amount_range") > 57)
)
