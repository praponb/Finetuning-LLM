from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status", "order_id").orderBy(F.col("delivered_date").desc_nulls_last(), "id")

enriched = (
    df_shipments
    .alias("src")
    .join(df_orders.alias("b"), F.col("src.order_id") == F.col("b.id"), "inner")
    .filter(F.col("b.shipping_fee") > 20)
    .select("src.*", F.col("b.discount"), F.col("b.order_date"), F.col("b.channel"))
)

deduped = (
    enriched
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
)

enriched_shipments = (
    deduped
    .alias("a")
    .join(df_orders.alias("o"), F.col("a.order_id") == F.col("o.id"), "inner")
    .select(
        "a.*",
        F.col("o.order_date").alias("order_order_date"),
        F.col("o.discount").alias("order_discount"),
        F.col("o.amount"),
    )
)

joined = (
    enriched_shipments
    .alias("src")
    .join(df_orders.alias("d"), F.col("src.order_id") == F.col("d.id"), "left")
    .select(
        "src.id",
        "src.order_id",
        "src.carrier",
        "src.status",
        "src.discount",
        "src.rn",
        F.col("d.channel").alias("order_channel"),
    )
)

result = (
    joined
    .select("id", "carrier", "status")
    .orderBy(F.col("id").desc_nulls_first())
    .limit(10)
)
