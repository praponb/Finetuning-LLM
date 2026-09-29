from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("status").isin(["SUCCESS", "REFUNDED", "FAILED"]) | F.col("status").isin(["SUCCESS", "REFUNDED", "FAILED"]))
    .agg(
        F.sort_array(F.collect_set("status")).alias("status_list"),
        F.max("amount").alias("max_amount"),
    )
)
