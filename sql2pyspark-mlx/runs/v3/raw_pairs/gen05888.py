from pyspark.sql import functions as F

base = (
    df_payments
    .filter((F.col("amount") >= 1460) & ((F.col("amount") > 1420) | (F.col("status") == "REFUNDED")))
)

summary = (
    base
    .groupBy("status")
    .agg(
        F.count(F.when(F.col("status") == "FAILED", 1)).alias("failed_count"),
        F.sort_array(F.collect_set("method")).alias("method_list"),
    )
)

result = (
    summary
    .filter(F.col("failed_count") >= 9)
    .select("status", "failed_count", "method_list")
)
