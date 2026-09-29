from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy("amount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("status").orderBy(F.col("shipping_fee").asc_nulls_last())
w3 = Window.partitionBy("status").orderBy(F.col("paid_at").cast("bigint")).rangeBetween(-2592000, Window.currentRow)
w_running = Window.partitionBy("order_id").orderBy(F.col("shipping_fee_sum_30d").desc(), "id")
w5 = Window.partitionBy("method")
w6 = Window.partitionBy("method", "order_id").orderBy(F.col("paid_at").cast("bigint")).rangeBetween(-2592000, Window.currentRow)
w_recent = Window.partitionBy("order_id", "method").orderBy(F.col("paid_at").cast("bigint")).rangeBetween(-2592000, Window.currentRow)
w8 = Window.partitionBy(F.month("paid_at")).orderBy("shipping_fee", "id")

base = (
    df_payments
    .filter((F.col("paid_at") > F.lit("2023-07-17").cast("date")) & F.col("order_id").isNotNull())
    .select(
        "*",
        F.trim("status").alias("clean_status"),
        F.coalesce("method", F.lit("DIRECT")).alias("method_filled"),
    )
)

joined = (
    base
    .alias("e")
    .join(df_orders.alias("p"), F.col("e.order_id") == F.col("p.id"), "left")
    .select("e.*", F.col("p.shipping_fee"))
)

enriched = (
    joined
    .select(
        "id",
        "order_id",
        "method",
        "amount",
        "paid_at",
        "clean_status",
        "method_filled",
        "shipping_fee",
        F.sum("shipping_fee").over(w).alias("cumulative_shipping_fee"),
        F.dense_rank().over(w2).alias("shipping_fee_dense_rank"),
        F.sum("shipping_fee").over(w3).alias("shipping_fee_sum_30d"),
    )
)

scored = (
    enriched
    .select(
        "id",
        "order_id",
        "method",
        "cumulative_shipping_fee",
        "shipping_fee_dense_rank",
        F.row_number().over(w_running).alias("seq_no"),
        F.sum("shipping_fee_dense_rank").over(w5).alias("method_sum_shipping_fee_dense_rank"),
        F.sum("amount").over(w6).alias("amount_sum_30d"),
        F.count("cumulative_shipping_fee").over(w_recent).alias("cumulative_shipping_fee_count_30d"),
        F.row_number().over(w8).alias("row_seq"),
    )
)

result = (
    scored
    .alias("s")
    .join(df_orders.alias("d"), F.col("s.order_id") == F.col("d.id"), "inner")
    .groupingSets(
        [["channel", "method"], ["channel"]],
        F.col("d.channel").alias("channel"),
        F.col("s.method").alias("method"),
    )
    .agg(
        F.count("*").alias("rows_total"),
        F.max(F.col("s.method_sum_shipping_fee_dense_rank")).alias("max_method_sum_shipping_fee_dense_rank"),
        F.grouping_id().alias("grouping_level"),
    )
    .orderBy(F.col("channel").desc_nulls_first())
)
