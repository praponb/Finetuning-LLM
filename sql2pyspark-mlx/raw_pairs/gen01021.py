from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("channel", "status")
w2 = Window.orderBy("amount", "id")
w3 = Window.orderBy("shipping_fee", "id").rowsBetween(-5, Window.currentRow)
w4 = Window.partitionBy(F.month("order_date"), "channel").orderBy(F.col("amount").desc())
w5 = Window.partitionBy(F.year("order_date"), "channel").orderBy("shipping_fee", "id").rowsBetween(-2, -1)
w_hist = Window.partitionBy("channel", "status").orderBy("corr_discount_amount", "id").rowsBetween(-3, Window.currentRow)
w7 = Window.partitionBy("status").orderBy("shipping_fee_avg_2_prior", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

enriched = (
    df_orders
    .select(
        "id",
        "customer_id",
        "order_date",
        "status",
        "channel",
        "amount",
        "discount",
        "shipping_fee",
        F.corr("discount", "amount").over(w).alias("corr_discount_amount"),
        F.lag("amount", 1).over(w2).alias("prev_amount"),
        F.avg("shipping_fee").over(w3).alias("shipping_fee_avg_5"),
        F.rank().over(w4).alias("amount_rank"),
        F.avg("shipping_fee").over(w5).alias("shipping_fee_avg_2_prior"),
    )
)

filtered = (
    enriched
    .filter((F.col("order_date") > F.lit("2023-03-24").cast("date")) & F.col("customer_id").isin([5, 15, 28, 30]))
    .select(
        "id",
        "status",
        "channel",
        "amount",
        "discount",
        "shipping_fee",
        "corr_discount_amount",
        "prev_amount",
        "amount_rank",
        "shipping_fee_avg_2_prior",
        F.coalesce("shipping_fee", F.lit(0)).alias("shipping_fee_filled"),
        F.col("shipping_fee_avg_5").cast("string").alias("shipping_fee_avg_5_str"),
    )
)

not_flagged = (
    filtered
    .alias("s")
    .join(df_shipments.alias("x"), (F.col("x.order_id") == F.col("s.id")) & (F.col("x.carrier") == "Flash"), "left_anti")
)

scored = (
    not_flagged
    .filter(F.col("prev_amount") > 920)
    .select(
        "id",
        "status",
        "shipping_fee",
        "corr_discount_amount",
        "prev_amount",
        "amount_rank",
        "shipping_fee_filled",
        "shipping_fee_avg_5_str",
        F.count("prev_amount").over(w_hist).alias("last4_count_prev_amount"),
        F.avg("prev_amount").over(w_hist).alias("last4_avg_prev_amount"),
        F.min("prev_amount").over(w_hist).alias("last4_min_prev_amount"),
        F.last("prev_amount", ignorenulls=True).over(w7).alias("last_known_prev_amount"),
    )
)

kept = (
    scored
    .alias("s")
    .join(df_shipments.alias("ch"), (F.col("ch.order_id") == F.col("s.id")) & (F.col("ch.order_id").isin([14, 27, 31])), "left_semi")
)

result = (
    kept
    .select("id", "status", "prev_amount", "last4_avg_prev_amount")
    .unpivot(
        ids=["id", "status"],
        values=["prev_amount", "last4_avg_prev_amount"],
        variableColumnName="measure",
        valueColumnName="val",
    )
    .filter(F.col("val").isNotNull())
)
