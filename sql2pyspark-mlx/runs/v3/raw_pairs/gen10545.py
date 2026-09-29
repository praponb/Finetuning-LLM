from pyspark.sql import functions as F

result = (
    df_sessions.alias("s")
    .join(df_web_events.alias("we"), F.col("we.session_id") == F.col("s.id"), "left")
    .filter(~F.col("s.browser").isin(["Chrome", "Safari"]) & F.col("we.page").isin(["/cart", "/product", "/home"]))
    .select(
        F.col("s.started_at"),
        F.col("s.customer_id"),
        F.col("s.country"),
        F.col("we.page"),
    )
)
