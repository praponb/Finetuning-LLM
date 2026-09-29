from pyspark.sql import functions as F

recent_payments = df_payments.filter(F.col("status") == "REFUNDED")

summary = (
    recent_payments
    .groupBy("order_id")
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("method"))).alias("method_values"),
        F.round(((100 * F.sum(F.when(F.col("method") == "card", 1).otherwise(0))) / F.count("*")), 1).alias("pct_card"),
    )
)

result = (
    summary
    .select("order_id", "method_values", "pct_card")
    .orderBy(F.col("order_id").desc())
    .limit(3)
)
