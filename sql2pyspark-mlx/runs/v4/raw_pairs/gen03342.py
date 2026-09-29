from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy(F.col("quantity").desc_nulls_last())
w2 = Window.partitionBy("product_id")
w3 = Window.orderBy(F.col("unit_price").desc(), "id")

scored = (
    df_order_items
    .withColumn("quantity_percent_rank", F.percent_rank().over(w))
    .withColumn("corr_unit_price_quantity", F.corr("unit_price", "quantity").over(w2))
    .withColumn("event_seq", F.row_number().over(w3))
)

remaining_keys = (
    scored
    .select("product_id")
    .exceptAll(
        df_order_items
        .filter(F.col("order_id") == 21)
        .select("product_id")
    )
)

verified = (
    scored
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.product_id") == F.col("k.product_id")), "inner")
    .select("s.*")
)

with_dims = (
    verified
    .alias("a")
    .join(df_orders.alias("p"), F.col("a.order_id") == F.col("p.id"), "left")
    .select("a.*", F.col("p.shipping_fee"), F.col("p.channel"), F.col("p.status"))
)

enriched = (
    with_dims
    .alias("e")
    .join(df_products.alias("d"), F.col("e.product_id") == F.col("d.id"), "inner")
    .select("e.*", F.col("d.brand"), F.col("d.launch_date"))
)

result = (
    enriched
    .groupBy(
        F.when(F.col("corr_unit_price_quantity") >= 0.74, "PREMIUM").otherwise("STANDARD").alias("corr_unit_price_quantity_tier"),
    )
    .agg(
        F.min("shipping_fee").alias("min_shipping_fee"),
        F.bit_and("quantity").alias("quantity_bit_and"),
        F.count("*").alias("row_count"),
    )
    .orderBy(F.col("quantity_bit_and").desc())
)
