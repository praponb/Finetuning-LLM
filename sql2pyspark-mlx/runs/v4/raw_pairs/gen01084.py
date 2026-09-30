from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy(F.col("unit_price").desc_nulls_first())
w2 = Window.partitionBy("product_id", "order_id")
w3 = Window.orderBy("unit_price", "id").rowsBetween(-5, Window.currentRow)
w4 = Window.partitionBy("order_id", "product_id").orderBy("quantity_filled", "id").rowsBetween(-5, -1)
w5 = Window.partitionBy("product_id")
w6 = Window.partitionBy("order_id").orderBy("quantity_filled_avg_5")

valid = (
    df_order_items
    .filter(F.col("order_id") == 37)
    .select(
        "id",
        "order_id",
        "product_id",
        "quantity",
        "unit_price",
        F.coalesce("quantity", F.lit(0)).alias("quantity_filled"),
    )
)

ranked = (
    valid
    .withColumn("rn", F.rank().over(w))
    .filter(F.col("rn") == 1)
)

enriched = (
    ranked
    .withColumn("corr_unit_price_quantity", F.corr("unit_price", "quantity").over(w2))
    .withColumn("quantity_filled_avg_5", F.avg("quantity_filled").over(w3))
    .withColumn("quantity_avg_5_prior", F.avg("quantity").over(w4))
    .withColumn("product_id_avg_unit_price", F.avg("unit_price").over(w5))
)

ranked_order_items = (
    enriched
    .withColumn("rank_in_group", F.rank().over(w6))
    .filter(F.col("rank_in_group") == 1)
)

joined = (
    ranked_order_items
    .alias("s")
    .join(df_products.alias("d"), F.col("s.product_id") == F.col("d.id"), "left")
    .select("s.*", F.col("d.product_name"), F.col("d.stock_qty"))
)

result = (
    joined
    .groupBy(
        "product_id",
        F.when(F.col("stock_qty") >= 398, "LARGE").otherwise("SMALL").alias("stock_qty_tier"),
    )
    .agg(
        F.bit_and("quantity").alias("quantity_bit_and"),
        F.countDistinct("id").alias("distinct_ids"),
        F.percentile_approx("quantity_filled", [0.25, 0.5, 0.75]).alias("quantity_filled_quartiles"),
        F.corr("quantity", "unit_price").alias("corr_quantity_unit_price"),
    )
)
