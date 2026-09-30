from pyspark.sql import functions as F

result = (
    df_payments
    .groupBy("method")
    .agg(
        F.sum(F.when(F.col("status") == "REFUNDED", F.col("amount")).otherwise(0)).alias("refunded_amount"),
        F.sort_array(F.collect_set("method")).alias("method_list"),
    )
    .orderBy(F.col("refunded_amount").desc())
)
