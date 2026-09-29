from pyspark.sql import functions as F

result = (
    df_web_events
    .groupBy("event_type")
    .pivot("page", ["/checkout", "/product"])
    .agg(F.sum("duration_sec"))
    .select(
        "event_type",
        F.col("/checkout").alias("checkout"),
        F.col("/product").alias("product"),
    )
)
