from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("carrier").orderBy(F.col("shipping_fee").asc_nulls_last(), "id")
w2 = Window.partitionBy("order_id", "carrier").orderBy(F.col("amount").desc(), "id")
w_running = Window.partitionBy("carrier", "order_id").orderBy("delivered_date", "id")

enriched = (
    df_shipments
    .alias("s")
    .join(df_orders.alias("b"), F.col("s.order_id") == F.col("b.id"), "left")
    .filter(F.col("b.status").isin(["RETURNED", "SHIPPED", "CANCELLED"]))
    .select("s.*", F.col("b.shipping_fee"))
)

ranked = (
    enriched
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("id", "order_id", "carrier", "shipped_date", "delivered_date")
)

enriched_shipments = (
    ranked
    .alias("src")
    .join(df_orders.alias("b"), F.col("src.order_id") == F.col("b.id"), "left")
    .select(
        "src.id",
        "src.order_id",
        "src.carrier",
        "src.delivered_date",
        F.col("b.amount"),
    )
)

base = (
    enriched_shipments
    .filter(F.col("carrier").isNotNull() & F.col("carrier").isin(["Flash", "FedEx", "Kerry"]))
    .withColumn("delivered_date_label", F.date_format("delivered_date", "yyyy-MM"))
)

result = (
    base
    .select(
        "id",
        "carrier",
        "delivered_date",
        "amount",
        "delivered_date_label",
        F.ntile(5).over(w2).alias("amount_bucket"),
        F.lead("amount", 2).over(w_running).alias("next_amount"),
    )
)
