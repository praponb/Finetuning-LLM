from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy(F.col("unit_price").desc_nulls_first(), "id")

base = (
    df_order_items
    .filter(F.col("product_id").isNull())
    .select(
        "*",
        F.round((F.col("unit_price") / F.col("quantity")), 2).alias("line_total"),
    )
)

joined = (
    base
    .alias("e")
    .join(df_orders.alias("d"), F.col("e.order_id") == F.col("d.id"), "inner")
    .select("e.*", F.col("d.channel"), F.col("d.status"), F.col("d.shipping_fee"))
)

ranked = (
    joined
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
)

valid = (
    ranked
    .filter(F.col("unit_price").isNotNull())
    .select("*", (F.col("quantity") + F.col("rn")).alias("quantity_plus_rn"))
)

result = (
    valid
    .groupBy(
        F.when(F.col("quantity_plus_rn") >= 64, "PREMIUM").otherwise("STANDARD").alias("quantity_plus_rn_tier"),
        "status",
    )
    .agg(
        F.stddev_samp("quantity").alias("quantity_stddev"),
        F.round(F.sum("shipping_fee") / F.nullif(F.count(F.when(F.col("channel") == "web", 1)), F.lit(0)), 2).alias("shipping_fee_per_web"),
        F.percentile_approx("line_total", 0.5).alias("line_total_p50"),
        F.size(F.collect_set("status")).alias("num_status"),
        F.round(F.sum("shipping_fee") / F.nullif(F.count(F.when(F.col("status") == "SHIPPED", 1)), F.lit(0)), 2).alias("shipping_fee_per_shipped"),
    )
)
