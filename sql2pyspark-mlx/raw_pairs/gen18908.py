from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy("discount", "id").rowsBetween(-3, Window.currentRow)
w2 = Window.orderBy("delivered_date", "id")

base = (
    df_shipments
    .filter(F.col("order_id").isNotNull())
    .select(
        "*",
        (F.datediff("delivered_date", "shipped_date") / 7).alias("weeks_shipped_date_to_delivered_date"),
        F.coalesce("carrier", F.lit("UNKNOWN")).alias("carrier_clean"),
    )
)

enriched = (
    base
    .alias("a")
    .join(df_orders.alias("d"), F.col("a.order_id") == F.col("d.id"), "left")
    .select(
        "a.id",
        "a.order_id",
        "a.carrier",
        "a.delivered_date",
        "a.status",
        "a.weeks_shipped_date_to_delivered_date",
        F.col("d.shipping_fee"),
        F.col("d.discount"),
        F.col("d.amount"),
    )
)

metrics = (
    enriched
    .select(
        "id",
        "order_id",
        "delivered_date",
        "discount",
        F.avg("shipping_fee").over(w).alias("last4_avg_shipping_fee"),
        F.min("shipping_fee").over(w).alias("last4_min_shipping_fee"),
        F.max("shipping_fee").over(w).alias("last4_max_shipping_fee"),
        F.lead("amount", 1).over(w2).alias("next_amount"),
    )
)

filtered = (
    metrics
    .filter((F.col("delivered_date") > F.lit("2023-04-03").cast("date")) & F.col("order_id").isNotNull())
    .withColumn("next_amount_whole", F.ceil("next_amount"))
)

result = (
    filtered
    .select(
        "id",
        "order_id",
        "last4_min_shipping_fee",
        "next_amount",
        "last4_max_shipping_fee",
    )
    .unpivot(
        ids=["id", "order_id"],
        values=["last4_min_shipping_fee", "next_amount", "last4_max_shipping_fee"],
        variableColumnName="metric_name",
        valueColumnName="reading",
    )
)
