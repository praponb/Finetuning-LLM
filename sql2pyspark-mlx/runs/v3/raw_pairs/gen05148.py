from pyspark.sql import functions as F

result = (
    df_sessions.alias("s")
    .join(df_web_events.filter(F.col("page").isin(["/search", "/product", "/checkout"])).alias("we"), F.col("s.id") == F.col("we.session_id"), "left_anti")
    .filter(F.col("s.customer_id").isNull())
    .select(
        F.col("s.id"),
        F.col("s.device"),
        F.col("s.customer_id"),
        F.col("s.browser"),
    )
    .orderBy("customer_id")
)
