from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_ordered = Window.partitionBy("status").orderBy("amount", "id")
w2 = Window.orderBy(F.col("shipping_fee").desc(), "id")
w3 = Window.partitionBy("status")
w4 = Window.partitionBy("order_id").orderBy("order_discount", "id")
w5 = Window.partitionBy(F.month("order_date"), "status").orderBy(F.col("shipping_fee").desc())
w6 = Window.partitionBy("status").orderBy("years_since_paid_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

scored = (
    df_payments
    .withColumn("next_amount", F.lead("amount").over(w_ordered))
    .withColumn("row_seq", F.row_number().over(w_ordered))
)

valid = (
    scored
    .filter((F.col("paid_at") >= F.lit("2023-10-20").cast("date")) & (F.col("status") != "REFUNDED"))
    .withColumn("method_upper", F.upper("method"))
    .withColumn("years_since_paid_at", (F.datediff(F.current_date(), "paid_at") / 365.25))
)

joined = (
    valid
    .alias("s")
    .join(df_orders.alias("d"), F.col("s.order_id") == F.col("d.id"), "left")
    .filter(F.col("d.store_id").isNull())
    .select("s.*", F.col("d.discount"), F.col("d.shipping_fee"))
)

enriched = (
    joined
    .alias("e")
    .join(df_orders.alias("b"), F.col("e.order_id").eqNullSafe(F.col("b.id")), "left")
    .select("e.*", F.col("b.discount").alias("order_discount"), F.col("b.order_date"))
)

metrics = (
    enriched
    .select(
        "id",
        "status",
        "next_amount",
        "row_seq",
        "method_upper",
        "years_since_paid_at",
        "discount",
        "shipping_fee",
        "order_date",
        F.ntile(3).over(w2).alias("shipping_fee_bucket"),
        F.round((F.col("amount") / F.sum("amount").over(w3)), 4).alias("amount_share"),
        F.lag("order_discount", 1).over(w4).alias("prev_order_discount"),
        F.rank().over(w5).alias("shipping_fee_rank"),
        F.first("order_discount", ignorenulls=True).over(w6).alias("first_known_order_discount"),
    )
)

stats = (
    metrics
    .groupBy(F.date_trunc("QUARTER", "order_date").alias("quarter_start"), "status")
    .agg(
        F.percentile_approx("first_known_order_discount", 0.9).alias("first_known_order_discount_p90"),
        F.round(F.sum("first_known_order_discount") / F.nullif(F.count(F.when(F.col("status") == "SUCCESS", 1)), F.lit(0)), 2).alias("first_known_order_discount_per_success"),
        F.sum(F.when(F.col("status") == "REFUNDED", F.col("next_amount")).otherwise(0)).alias("refunded_next_amount"),
        F.percentile_approx("shipping_fee", 0.5).alias("shipping_fee_p50"),
    )
    .filter(F.col("first_known_order_discount_p90") > 0)
)

result = (
    stats
    .select(
        "quarter_start",
        "status",
        "first_known_order_discount_per_success",
        "shipping_fee_p50",
    )
    .orderBy(F.col("status").asc_nulls_first())
)
