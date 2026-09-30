from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy("quantity", "id")
w2 = Window.partitionBy("product_id", "order_id")
w_grp = Window.partitionBy("order_id")
w_recent = Window.partitionBy("order_id").orderBy("row_seq", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

metrics = (
    df_order_items
    .filter(F.col("product_id") == 40)
    .select(
        "id",
        "order_id",
        F.row_number().over(w).alias("row_seq"),
        F.corr("quantity", "unit_price").over(w2).alias("corr_quantity_unit_price"),
    )
)

base = (
    metrics
    .filter(F.col("order_id") == 28)
    .withColumn("row_seq_minus_corr_quantity_unit_price", (F.col("row_seq") - F.col("corr_quantity_unit_price")))
)

enriched = (
    base
    .alias("e")
    .join(df_orders.alias("b"), F.col("e.order_id") == F.col("b.id"), "left")
    .select("e.id", "e.order_id", "e.row_seq", F.col("b.shipping_fee"))
)

valid = (
    enriched
    .filter(F.col("row_seq") == 8)
    .select(
        "*",
        (F.col("shipping_fee") - F.col("row_seq")).alias("shipping_fee_minus_row_seq"),
    )
)

with_windows = (
    valid
    .select(
        "id",
        "row_seq",
        "shipping_fee",
        "shipping_fee_minus_row_seq",
        F.max("shipping_fee").over(w_grp).alias("order_id_max_shipping_fee"),
        F.last("row_seq", ignorenulls=True).over(w_recent).alias("last_known_row_seq"),
        F.stddev_samp("shipping_fee_minus_row_seq").over(w_grp).alias("order_id_stddev_samp_shipping_fee_minus_row_seq"),
    )
)

report = (
    with_windows
    .select(
        "id",
        "shipping_fee",
        "order_id_max_shipping_fee",
        "order_id_stddev_samp_shipping_fee_minus_row_seq",
    )
    .orderBy(F.col("order_id_stddev_samp_shipping_fee_minus_row_seq").desc_nulls_last())
)

result = (
    report
    .select("id", "shipping_fee", "order_id_stddev_samp_shipping_fee_minus_row_seq")
    .orderBy(F.col("id").desc())
)
