from pyspark.sql import functions as F

filtered = (
    df_web_events
    .filter(F.col("page").isNotNull())
    .select(
        "id",
        "session_id",
        "page",
        "duration_sec",
        F.initcap(F.trim("event_type")).alias("event_type_title"),
        F.col("duration_sec").cast("double").alias("duration_sec_str"),
    )
)

result = (
    filtered
    .groupBy("session_id")
    .pivot("page", ["/product", "/cart"])
    .agg(F.avg("duration_sec"))
    .select(
        "session_id",
        F.col("/product").alias("product"),
        F.col("/cart").alias("cart"),
    )
)
