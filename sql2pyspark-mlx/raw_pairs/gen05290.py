from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("status") == "REFUNDED")
    .groupBy(F.quarter("paid_at").alias("paid_quarter"), "method")
    .agg(
        F.sort_array(F.collect_set("method")).alias("method_list"),
        F.max("amount").alias("max_amount"),
    )
    .filter(F.col("max_amount") < 1190)
    .select("paid_quarter", "method", "method_list", "max_amount")
    .orderBy("max_amount", F.col("method").desc(), "paid_quarter")
    .limit(5)
)
