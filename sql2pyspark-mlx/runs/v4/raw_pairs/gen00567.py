from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("channel").orderBy("order_date", "id").rowsBetween(-2, -1)
w2 = Window.partitionBy("order_id").orderBy("quantity", "id").rowsBetween(-6, -1)
w3 = Window.partitionBy("order_id").orderBy("order_date", "id")
w4 = Window.partitionBy("product_id").orderBy(F.col("order_date").desc_nulls_first(), "id")

joined = (
    df_order_items
    .alias("e")
    .join(df_orders.alias("p"), F.col("e.order_id") == F.col("p.id"), "inner")
    .select("e.*", F.col("p.channel"), F.col("p.order_date"))
)

with_windows = (
    joined
    .withColumn("unit_price_avg_2_prior", F.avg("unit_price").over(w))
    .withColumn("quantity_avg_6_prior", F.avg("quantity").over(w2))
    .withColumn("prev_unit_price", F.lag("unit_price", 2).over(w3))
)

with_dims = (
    with_windows
    .alias("s")
    .join(df_orders.alias("b"), F.col("s.order_id") == F.col("b.id"), "inner")
    .select("s.*", F.col("b.discount"), F.col("b.amount"), F.col("b.shipping_fee"))
)

enriched = (
    with_dims
    .alias("src")
    .join(df_products.alias("p"), F.col("src.product_id") == F.col("p.id"), "left")
    .select("src.*", F.col("p.product_name"), F.col("p.cost"))
)

deduped = (
    enriched
    .withColumn("dedup_rank", F.row_number().over(w4))
    .filter(F.col("dedup_rank") == 1)
)

result = (
    deduped
    .groupBy(F.date_trunc("year", "order_date").alias("year_start"))
    .agg(
        F.avg(F.when(F.col("channel") == "mobile", F.col("unit_price"))).alias("mobile_avg_unit_price"),
        F.round(F.avg("unit_price"), 2).alias("avg_unit_price"),
        F.avg(F.when(F.col("channel") == "mobile", F.col("discount"))).alias("mobile_avg_discount"),
    )
    .orderBy(F.col("year_start").desc())
)
