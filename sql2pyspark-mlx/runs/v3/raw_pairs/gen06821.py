from pyspark.sql import functions as F

session_summary = (
    df_web_events
    .groupBy("session_id")
    .agg(
        F.sum(F.when(F.col("page") == "/product", 1).otherwise(0)).alias("product_count"),
        F.count(F.when(F.col("event_type") == "purchase", 1)).alias("purchase_count"),
    )
)

result = (
    df_sessions.alias("s")
    .join(session_summary.alias("ss"), F.col("s.id") == F.col("ss.session_id"), "inner")
    .filter(F.col("ss.purchase_count") > 4)
    .select(
        F.col("s.id"),
        F.col("s.customer_id"),
        F.col("ss.product_count"),
        F.col("ss.purchase_count"),
    )
    .orderBy(F.col("customer_id").desc(), "id")
    .limit(10)
)
