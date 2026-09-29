from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy("amount", "id")
w2 = Window.partitionBy("order_id").orderBy(F.col("order_date").asc_nulls_last(), "id")
w3 = Window.partitionBy("method").orderBy(F.col("paid_at").cast("bigint")).rangeBetween(-7776000, Window.currentRow)
w4 = Window.orderBy(F.col("paid_at").cast("bigint")).rangeBetween(-2592000, Window.currentRow)
w5 = Window.partitionBy("method").orderBy("rank_in_group", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

base = (
    df_payments
    .filter(~F.col("status").isin(["SUCCESS", "FAILED"]))
    .select(
        "*",
        F.coalesce("amount", F.lit(0)).alias("amount_filled"),
        F.coalesce("method", F.lit("NONE")).alias("method_clean"),
    )
)

metrics = (
    base
    .filter(F.col("order_id").isin([14, 29, 32]))
    .select(
        "*",
        F.lag("amount_filled").over(w).alias("prev_amount_filled"),
        F.lead("amount").over(w).alias("next_amount"),
        F.row_number().over(w).alias("event_seq"),
    )
)

joined = (
    metrics
    .alias("s")
    .join(df_orders.alias("o"), F.col("s.order_id") == F.col("o.id"), "left")
    .select("s.*", F.col("o.order_date"))
)

ranked = (
    joined
    .withColumn("rank_in_group", F.row_number().over(w2))
    .filter(F.col("rank_in_group") == 1)
)

with_windows = (
    ranked
    .withColumn("amount_max_90d", F.max("amount").over(w3))
    .withColumn("amount_max_30d", F.max("amount").over(w4))
    .withColumn("running_amount_filled", F.sum("amount_filled").over(w5))
)

result = (
    with_windows
    .select(
        "id",
        "order_id",
        "method",
        "status",
        "amount_filled",
        "method_clean",
        "prev_amount_filled",
        "next_amount",
        "amount_max_90d",
    )
    .orderBy(F.col("method").desc_nulls_first())
)
