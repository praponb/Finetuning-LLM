from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy(F.col("line_total").desc(), "id")
w_recent = Window.partitionBy("order_id").orderBy(F.col("quantity").asc_nulls_last())
w_grp = Window.partitionBy("order_id", "product_id")
w4 = Window.partitionBy("order_id", "product_id").orderBy("unit_price", "id")
w5 = Window.partitionBy("order_id").orderBy("shipping_fee", "id")
w6 = Window.partitionBy("order_id").orderBy(F.col("prev_shipping_fee").desc_nulls_last(), "id")

base = (
    df_order_items
    .filter(F.col("product_id").isNotNull())
    .withColumn("line_total", F.round((F.col("unit_price") / F.col("quantity")), 2))
)

latest = (
    base
    .withColumn("rank_in_group", F.row_number().over(w))
    .filter(F.col("rank_in_group") == 1)
)

metrics = (
    latest
    .filter(F.col("quantity") > 5)
    .select(
        "id",
        "order_id",
        "unit_price",
        "line_total",
        "rank_in_group",
        F.rank().over(w_recent).alias("quantity_rank"),
        F.corr("quantity", "unit_price").over(w_grp).alias("corr_quantity_unit_price"),
        F.lag("quantity", 1).over(w4).alias("prev_quantity"),
    )
)

enriched = (
    metrics
    .alias("s")
    .join(df_orders.alias("p"), F.col("s.order_id") == F.col("p.id"), "left")
    .select(
        "s.id",
        "s.order_id",
        "s.unit_price",
        "s.corr_quantity_unit_price",
        "s.prev_quantity",
        F.col("p.amount"),
    )
)

joined = (
    enriched
    .alias("s")
    .join(df_orders.alias("b"), F.col("s.order_id") == F.col("b.id"), "inner")
    .select("s.*", F.col("b.shipping_fee"), F.col("b.discount"))
)

scored = (
    joined
    .select(
        "*",
        F.lag("corr_quantity_unit_price").over(w5).alias("prev_corr_quantity_unit_price"),
        F.lag("shipping_fee").over(w5).alias("prev_shipping_fee"),
        F.row_number().over(w5).alias("row_seq"),
    )
)

ranked = (
    scored
    .withColumn("row_num", F.row_number().over(w6))
    .filter(F.col("row_num") == 1)
    .select(
        "id",
        "order_id",
        "corr_quantity_unit_price",
        "prev_quantity",
        "amount",
        "discount",
        "prev_corr_quantity_unit_price",
        "prev_shipping_fee",
        "row_seq",
    )
)

result = (
    ranked
    .rollup(
        "order_id",
        F.when(F.col("prev_quantity") >= 9, "LARGE").otherwise("SMALL").alias("prev_quantity_tier"),
    )
    .agg(
        F.percentile_approx("prev_corr_quantity_unit_price", [0.25, 0.5, 0.75]).alias("prev_corr_quantity_unit_price_quartiles"),
        F.avg("corr_quantity_unit_price").alias("avg_corr_quantity_unit_price"),
        F.countDistinct("order_id").alias("distinct_order_ids"),
        F.sum("row_seq").alias("total_row_seq"),
        F.percentile_approx("prev_shipping_fee", 0.9).alias("prev_shipping_fee_p90"),
        F.min("prev_quantity").alias("min_prev_quantity"),
    )
    .filter(F.col("prev_shipping_fee_p90") > 0)
    .orderBy("min_prev_quantity")
)
