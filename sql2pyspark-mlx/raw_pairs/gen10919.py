from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy("quantity", "id")

joined = (
    df_order_items
    .alias("e")
    .join(df_orders.alias("o"), F.col("e.order_id") == F.col("o.id"), "left")
    .filter(F.col("o.customer_id").isin([7, 36, 39]))
    .select("e.id", "e.order_id", "e.quantity", F.col("o.status"))
)

deduped = (
    joined
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") == 1)
)

remaining_keys = (
    deduped
    .select("id", "order_id")
    .subtract(
        df_order_items
        .filter(F.col("order_id").isin([25, 34, 37]))
        .select("id", "order_id")
    )
)

reconciled = (
    deduped
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.order_id") == F.col("k.order_id")), "inner")
    .select("s.*")
)

enriched = (
    reconciled
    .alias("src")
    .join(df_orders.alias("p"), F.col("src.order_id") == F.col("p.id"), "left")
    .select(
        "src.id",
        "src.order_id",
        "src.status",
        F.col("p.shipping_fee"),
        F.col("p.discount"),
        F.col("p.amount"),
    )
)

with_dims = (
    enriched
    .alias("src")
    .join(df_orders.alias("d"), F.col("src.order_id") == F.col("d.id"), "inner")
    .select("src.id", "src.order_id", "src.discount", "src.amount", F.col("d.channel"))
)

daily = (
    with_dims
    .groupBy(
        F.when(F.col("discount") >= 0.36, "LARGE").otherwise("SMALL").alias("discount_tier"),
    )
    .agg(
        F.sum(F.when(F.col("channel") == "partner", F.col("amount")).otherwise(0)).alias("partner_amount"),
        F.count(F.when(F.col("channel") == "store", 1)).alias("store_count"),
    )
)

result = daily.select("discount_tier", "store_count")
