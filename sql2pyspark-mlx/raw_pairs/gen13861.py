from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_hist = Window.partitionBy("order_id")
w2 = Window.partitionBy(F.month("paid_at"), "order_id").orderBy(F.col("amount").asc_nulls_first())
w3 = Window.partitionBy("order_id").orderBy("amount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("order_id").orderBy(F.col("amount").desc_nulls_last())
w5 = Window.partitionBy("channel").orderBy(F.col("running_amount").desc(), "id")

cleaned = (
    df_payments
    .filter(F.col("paid_at") > F.lit("2023-10-06").cast("date"))
    .select(
        "id",
        "order_id",
        "method",
        "amount",
        "paid_at",
        "status",
        F.date_format("paid_at", "yyyy-MM-dd").alias("paid_at_label"),
        F.coalesce("method", F.lit("N/A")).alias("resolved_method"),
    )
)

scored = (
    cleaned
    .select(
        "*",
        F.min("amount").over(w_hist).alias("order_id_min_amount"),
        F.dense_rank().over(w2).alias("amount_dense_rank"),
        F.sum("amount").over(w3).alias("running_amount"),
    )
)

with_dims = (
    scored
    .alias("a")
    .join(df_orders.alias("b"), F.col("a.order_id") == F.col("b.id"), "left")
    .select("a.*", F.col("b.channel"), F.col("b.order_date"))
)

ranked = (
    with_dims
    .withColumn("dedup_rank", F.rank().over(w4))
    .filter(F.col("dedup_rank") == 1)
)

deduped = (
    ranked
    .withColumn("dedup_rank_2", F.row_number().over(w5))
    .filter(F.col("dedup_rank_2") == 1)
)

result = (
    deduped
    .alias("src")
    .join(df_orders.alias("o"), F.col("src.order_id") == F.col("o.id"), "left")
    .groupingSets(
        [["order_channel", "order_status", "method"], ["order_channel", "order_status"], ["order_channel"], []],
        F.col("o.channel").alias("order_channel"),
        F.col("o.status").alias("order_status"),
        F.col("src.method").alias("method"),
    )
    .agg(
        F.count("*").alias("num_rows"),
        F.min(F.col("src.amount")).alias("min_amount"),
        F.round(F.sum(F.col("src.order_id_min_amount")), 2).alias("total_order_id_min_amount"),
    )
    .orderBy(F.col("total_order_id_min_amount").asc_nulls_last())
)
