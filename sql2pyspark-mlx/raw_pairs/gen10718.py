from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy(F.col("delivered_date").asc_nulls_first(), "id")
w2 = Window.partitionBy("status").orderBy(F.col("cost").desc(), "id")
w3 = Window.partitionBy("status", "carrier").orderBy("cost")
w4 = Window.partitionBy("carrier").orderBy("shipped_date", "id")

base = (
    df_shipments
    .filter(F.col("carrier").isin(["UPS", "DHL"]))
    .select(
        "id",
        "order_id",
        "carrier",
        "shipped_date",
        "delivered_date",
        "cost",
        "status",
        F.upper("status").alias("status_upper"),
        F.coalesce("carrier", F.lit("N/A")).alias("resolved_carrier"),
    )
)

ranked = (
    base
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
)

latest = (
    ranked
    .withColumn("rn_2", F.row_number().over(w2))
    .filter(F.col("rn_2") == 1)
)

metrics = (
    latest
    .select(
        "*",
        F.cume_dist().over(w3).alias("cost_cume_dist"),
        F.row_number().over(w4).alias("event_seq"),
    )
)

enriched = (
    metrics
    .alias("s")
    .join(df_orders.alias("p"), F.col("s.order_id") == F.col("p.id"), "left")
    .filter(F.col("p.channel").isin(["partner", "mobile"]))
    .select("s.*", F.col("p.channel"), F.col("p.amount"))
)

result = (
    enriched
    .groupBy("carrier")
    .pivot("channel", ["mobile", "partner"])
    .agg(F.avg("event_seq"))
)
