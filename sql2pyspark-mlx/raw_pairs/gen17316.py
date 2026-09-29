from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.order_date").desc(), F.col("c.id").desc())
w2 = Window.partitionBy("city").orderBy("opened_date", "id")
w3 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.last_restock").desc(), F.col("c.id").desc())
w4 = Window.partitionBy("city").orderBy(F.col("sqft").desc_nulls_first(), "id")
w_part = Window.partitionBy("city").orderBy(F.col("order_shipping_fee").desc(), "id")
w6 = Window.partitionBy("city").orderBy("sqft", "id").rowsBetween(-5, Window.currentRow)
w7 = Window.partitionBy("city")

with_latest = (
    df_stores
    .alias("s")
    .join(df_orders.alias("c"), (F.col("c.store_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.shipping_fee").alias("order_shipping_fee"))
)

deduped = (
    with_latest
    .withColumn("row_num", F.row_number().over(w2))
    .filter(F.col("row_num") == 1)
)

with_last = (
    deduped
    .alias("s")
    .join(df_inventory.alias("c"), (F.col("c.store_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w3))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.city",
        "s.sqft",
        "s.order_shipping_fee",
        F.col("c.quantity").alias("inventory_quantity"),
        F.col("c.last_restock").alias("inventory_last_restock"),
    )
)

deduped_stores = (
    with_last
    .withColumn("rn", F.row_number().over(w4))
    .filter(F.col("rn") == 1)
    .select("id", "city", "sqft", "order_shipping_fee", "inventory_last_restock")
)

base = (
    deduped_stores
    .filter(F.col("sqft") != 7871)
    .withColumn("order_shipping_fee_band", F.when(F.col("order_shipping_fee") >= 37, "High").when(F.col("order_shipping_fee") >= 5, "Medium").otherwise("Low"))
    .withColumn("order_shipping_fee_minus_sqft", (F.col("order_shipping_fee") - F.col("sqft")))
)

with_windows = (
    base
    .filter(F.col("sqft") != 13410)
    .select(
        "*",
        F.ntile(5).over(w_part).alias("order_shipping_fee_bucket"),
        F.avg("sqft").over(w6).alias("sqft_avg_5"),
        F.min("sqft").over(w7).alias("city_min_sqft"),
        F.row_number().over(w_part).alias("seq_no"),
    )
)

latest_match = (
    with_windows
    .alias("s")
    .join(df_inventory.alias("c"), (F.col("c.store_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w3))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.city",
        "s.sqft",
        "s.order_shipping_fee",
        "s.order_shipping_fee_band",
        "s.order_shipping_fee_minus_sqft",
        "s.order_shipping_fee_bucket",
        "s.sqft_avg_5",
        "s.seq_no",
        F.col("c.last_restock").alias("inventory_last_restock_2"),
        F.col("c.quantity").alias("inventory_quantity"),
    )
)

result = (
    latest_match
    .groupBy(F.date_trunc("WEEK", "inventory_last_restock_2").alias("week_start"))
    .agg(
        F.size(F.collect_set("city")).alias("num_cities"),
        F.round(F.avg("inventory_quantity"), 2).alias("avg_inventory_quantity"),
    )
    .filter(F.col("avg_inventory_quantity") >= 0)
)
