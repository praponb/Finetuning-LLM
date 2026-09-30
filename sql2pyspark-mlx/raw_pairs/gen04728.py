from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.amount").desc(), F.col("c.id").desc())

base = (
    df_orders
    .filter((F.col("order_date") >= F.to_date(F.lit("2023-06-14"))) & F.col("channel").isNotNull())
    .select(
        "id",
        "store_id",
        "order_date",
        "discount",
        "shipping_fee",
        F.least("amount", "discount").alias("least_amount_discount"),
    )
)

not_flagged = (
    base
    .alias("s")
    .join(df_shipments.alias("ch"), (F.col("ch.order_id") == F.col("s.id")) & (F.col("ch.carrier") == "UPS"), "left_anti")
)

eligible = (
    not_flagged
    .alias("s")
    .join(df_order_items.alias("x"), (F.col("x.order_id") == F.col("s.id")) & (F.col("x.unit_price") > 380), "left_semi")
)

eligible_orders = (
    eligible
    .alias("s")
    .join(df_shipments.alias("x"), (F.col("x.order_id") == F.col("s.id")) & (F.col("x.delivered_date") <= F.col("s.order_date")), "left_anti")
)

with_latest = (
    eligible_orders
    .alias("s")
    .join(df_payments.alias("c"), (F.col("c.order_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.status"))
)

totals = (
    with_latest
    .groupBy(
        F.when(F.col("shipping_fee") >= 36, "HIGH").otherwise("LOW").alias("shipping_fee_tier"),
        F.date_trunc("YEAR", "order_date").alias("year_start"),
    )
    .agg(
        F.sum(F.when(F.col("status") == "FAILED", F.col("least_amount_discount"))).alias("failed_total_least_amount_discount"),
        F.percentile_approx("shipping_fee", 0.95).alias("shipping_fee_p95"),
        F.percentile_approx("discount", 0.5).alias("discount_p50"),
    )
)

result = (
    totals
    .select(
        "shipping_fee_tier",
        "year_start",
        "failed_total_least_amount_discount",
        "shipping_fee_p95",
    )
    .orderBy(F.col("shipping_fee_p95").asc_nulls_last())
)
