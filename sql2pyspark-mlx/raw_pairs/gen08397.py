from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status", "order_id").orderBy(F.col("shipping_fee").desc(), "id")
w2 = Window.partitionBy("status", "order_id").orderBy(F.col("shipping_fee").desc_nulls_last())
w3 = Window.partitionBy("status").orderBy("shipping_fee", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("status").orderBy(F.col("years_since_shipped_date").desc(), "id")
w5 = Window.partitionBy("channel").orderBy(F.col("discount").asc_nulls_last(), "id")
w6 = Window.partitionBy("channel").orderBy("shipping_fee_bucket", "id").rowsBetween(-2, Window.currentRow)

base = (
    df_shipments
    .filter((F.col("shipped_date") >= F.to_date(F.lit("2023-06-21"))) & F.col("order_id").isNotNull())
    .select(
        "id",
        "order_id",
        "shipped_date",
        "delivered_date",
        "cost",
        "status",
        (F.datediff(F.current_date(), "shipped_date") / 365).alias("years_since_shipped_date"),
        F.coalesce("carrier", F.lit("UNKNOWN")).alias("carrier_filled"),
    )
)

with_dims = (
    base
    .alias("s")
    .join(df_orders.alias("b"), F.col("s.order_id") == F.col("b.id"), "left")
    .select(
        "s.id",
        "s.order_id",
        "s.delivered_date",
        "s.status",
        "s.years_since_shipped_date",
        "s.carrier_filled",
        F.col("b.order_date"),
        F.col("b.shipping_fee"),
    )
)

metrics = (
    with_dims
    .filter(F.col("status") == "DELIVERED")
    .select(
        "id",
        "order_id",
        "carrier_filled",
        "order_date",
        "shipping_fee",
        F.ntile(3).over(w).alias("shipping_fee_bucket"),
        F.cume_dist().over(w2).alias("shipping_fee_cume_dist"),
        F.sum("years_since_shipped_date").over(w3).alias("running_years_since_shipped_date"),
        F.ntile(3).over(w4).alias("years_since_shipped_date_bucket"),
    )
)

with_dims_shipments = (
    metrics
    .alias("s")
    .join(df_orders.alias("b"), F.col("s.order_id").eqNullSafe(F.col("b.id")), "inner")
    .select("s.*", F.col("b.status"), F.col("b.discount"), F.col("b.channel"))
)

latest = (
    with_dims_shipments
    .withColumn("row_num", F.row_number().over(w5))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

with_windows = (
    latest
    .select(
        "id",
        "order_id",
        "shipping_fee",
        "shipping_fee_cume_dist",
        "years_since_shipped_date_bucket",
        "status",
        "discount",
        F.min("discount").over(w6).alias("last3_min_discount"),
        F.count("discount").over(w6).alias("last3_count_discount"),
        F.sum("discount").over(w6).alias("last3_sum_discount"),
        F.avg("discount").over(w6).alias("last3_avg_discount"),
    )
)

result = (
    with_windows
    .select(
        "id",
        "order_id",
        "shipping_fee",
        "shipping_fee_cume_dist",
        "years_since_shipped_date_bucket",
        "status",
        "discount",
        "last3_count_discount",
        "last3_sum_discount",
        F.when(F.col("status").rlike("^(RETURNED|COMPLETED)$"), "PRIORITY").when(F.col("status").rlike("^(PENDING|CANCELLED)$"), "GROUP_A").otherwise("EXTENDED").alias("status_zone"),
    )
    .orderBy(F.col("years_since_shipped_date_bucket").asc_nulls_first())
)
