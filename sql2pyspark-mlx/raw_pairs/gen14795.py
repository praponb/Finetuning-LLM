from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_running = Window.partitionBy("order_id", "status").orderBy("amount", "id")
w2 = Window.partitionBy("order_id").orderBy("cost", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

filtered = (
    df_shipments
    .filter(F.col("shipped_date") <= "2025-09-01")
    .withColumn("status_carrier_key", F.concat_ws("-", "status", "carrier"))
    .withColumn("days_shipped_date_to_delivered_date", F.datediff("delivered_date", "shipped_date"))
    .withColumn("carrier_filled", F.coalesce("carrier", F.lit("OTHER")))
)

joined = (
    filtered
    .alias("src")
    .join(df_orders.alias("b"), F.col("src.order_id") == F.col("b.id"), "left")
    .filter(F.col("b.channel").isNotNull())
    .select(
        "src.id",
        "src.order_id",
        "src.carrier",
        "src.cost",
        "src.status",
        "src.status_carrier_key",
        "src.carrier_filled",
        F.col("b.amount"),
        F.col("b.order_date"),
    )
)

with_windows = (
    joined
    .select(
        "*",
        F.lag("cost").over(w_running).alias("prev_cost"),
        F.lead("cost").over(w_running).alias("next_cost"),
        F.lead("amount").over(w_running).alias("next_amount"),
        F.lag("amount").over(w_running).alias("prev_amount"),
        F.row_number().over(w_running).alias("seq_no"),
        F.sum("amount").over(w2).alias("running_amount"),
    )
)

joined_shipments = (
    with_windows
    .alias("src")
    .join(df_orders.alias("o"), F.col("src.order_id") == F.col("o.id"), "left")
    .select(
        "src.id",
        "src.order_id",
        "src.carrier",
        "src.amount",
        "src.prev_cost",
        "src.seq_no",
        F.col("o.status").alias("order_status"),
        F.col("o.discount"),
    )
)

enriched = (
    joined_shipments
    .alias("e")
    .join(df_orders.alias("d"), F.col("e.order_id") == F.col("d.id"), "left")
    .select("e.*", F.col("d.discount").alias("order_discount"), F.col("d.order_date"))
)

result = (
    enriched
    .select("id", "carrier", "amount", "prev_cost", "seq_no", "order_discount")
    .orderBy("order_discount", F.col("id").desc())
)
