from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("segment").orderBy(F.col("end_date_period").desc())

with_dims = (
    df_subscriptions
    .alias("src")
    .join(df_customers.alias("b"), F.col("src.customer_id") == F.col("b.id"), "left")
    .select(
        "src.id",
        "src.customer_id",
        "src.end_date",
        F.col("b.segment"),
        F.col("b.age"),
    )
)

filtered = (
    with_dims
    .filter(F.col("end_date") >= F.to_date(F.lit("2023-05-18")))
    .select(
        "id",
        "customer_id",
        "segment",
        F.date_trunc("year", "end_date").alias("end_date_period"),
        F.coalesce("segment", F.lit("Unknown")).alias("segment_filled"),
    )
)

latest = (
    filtered
    .withColumn("latest_rank", F.rank().over(w))
    .filter(F.col("latest_rank") == 1)
)

with_dims_subscriptions = (
    latest
    .alias("e")
    .join(df_customers.alias("p"), F.col("e.customer_id") == F.col("p.id"), "inner")
    .select(
        "e.id",
        "e.customer_id",
        "e.end_date_period",
        "e.segment_filled",
        "e.latest_rank",
        F.col("p.email"),
        F.col("p.name"),
    )
)

matched_keys = (
    with_dims_subscriptions
    .select("id", "customer_id")
    .subtract(
        df_subscriptions
        .filter(F.col("end_date") < "2024-05-12")
        .select("id", "customer_id")
    )
)

reconciled = (
    with_dims_subscriptions
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.customer_id") == F.col("k.customer_id")), "inner")
    .select("s.*")
)

result = (
    reconciled
    .select("id", "customer_id", "end_date_period", "latest_rank", "name")
    .orderBy(F.col("customer_id").desc(), F.col("id").desc())
)
