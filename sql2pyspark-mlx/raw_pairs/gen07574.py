from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country").orderBy("started_at", "id")
w2 = Window.partitionBy("country").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("city").orderBy("signup_date", "id")
shipping_fee_by_customer_id = df_orders.groupBy("customer_id").agg(F.sum("shipping_fee").alias("sum_order_shipping_fee"))

kept = (
    df_sessions
    .alias("s")
    .join(df_web_events.alias("x"), (F.col("x.session_id") == F.col("s.id")) & (F.col("x.event_type") == "add_to_cart"), "left_anti")
)

flagged = (
    kept
    .withColumn("is_new_session", F.when(F.col("started_at") - F.lag("started_at").over(w) > F.expr("INTERVAL 7 DAYS"), 1).otherwise(0))
)

sessions_sessions = (
    flagged
    .select(
        "id",
        "customer_id",
        "started_at",
        "country",
        F.sum("is_new_session").over(w2).alias("burst_id"),
    )
)

with_dims = (
    sessions_sessions
    .alias("e")
    .join(df_customers.alias("d"), F.col("e.customer_id") == F.col("d.id"), "inner")
    .select(
        "e.id",
        "e.customer_id",
        "e.started_at",
        "e.burst_id",
        F.col("d.city"),
        F.col("d.signup_date"),
        F.col("d.country").alias("customer_country"),
    )
)

deduped = (
    with_dims
    .withColumn("latest_rank", F.row_number().over(w3))
    .filter(F.col("latest_rank") == 1)
)

result = (
    deduped
    .join(shipping_fee_by_customer_id, on="customer_id", how="left")
    .select(
        "id",
        "customer_id",
        "started_at",
        "burst_id",
        "city",
        "signup_date",
        "customer_country",
        "latest_rank",
        F.col("sum_order_shipping_fee"),
    )
    .orderBy("customer_country")
)
