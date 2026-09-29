from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.event_time").desc(), F.col("c.id").desc())
amount_by_customer_id = df_orders.groupBy("customer_id").agg(F.max("amount").alias("max_order_amount"))

valid = (
    df_sessions
    .filter(F.col("started_at") >= F.to_timestamp(F.lit("2023-02-20 07:00:00")))
    .select(
        "id",
        "customer_id",
        "device",
        "started_at",
        "country",
        F.concat_ws("-", "device", "browser").alias("device_browser_key"),
    )
)

kept = (
    valid
    .alias("s")
    .join(df_web_events.alias("x"), (F.col("x.session_id") == F.col("s.id")) & (F.col("x.event_time") >= F.col("s.started_at")), "left_anti")
)

with_dims = (
    kept
    .alias("a")
    .join(df_customers.alias("b"), F.col("a.customer_id") == F.col("b.id"), "left")
    .filter(~F.col("b.country").isin(["US", "TH"]))
    .select(
        "a.id",
        "a.customer_id",
        F.col("b.email"),
        F.col("b.segment"),
        F.col("b.country").alias("customer_country"),
    )
)

enriched = (
    with_dims
    .alias("a")
    .join(df_customers.alias("d"), F.col("a.customer_id") == F.col("d.id"), "left")
    .select("a.*", F.col("d.city"), F.col("d.email").alias("customer_email"))
)

latest_match = (
    enriched
    .alias("s")
    .join(df_web_events.alias("c"), (F.col("c.session_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.event_time"))
)

result = (
    latest_match
    .join(amount_by_customer_id, on="customer_id", how="left")
    .select(
        "id",
        "customer_id",
        "email",
        "segment",
        "customer_country",
        "customer_email",
        F.col("max_order_amount"),
    )
    .orderBy("id")
)
