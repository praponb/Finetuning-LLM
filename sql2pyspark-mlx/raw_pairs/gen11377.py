from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id", "method").orderBy(F.col("amount").asc_nulls_first(), "id")
w2 = Window.orderBy("amount", "id").rowsBetween(-5, Window.currentRow)
w3 = Window.orderBy("amount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("order_id", "status").orderBy("order_amount", "id").rowsBetween(-6, Window.currentRow)

base = (
    df_payments
    .filter((F.col("paid_at") > F.lit("2023-09-11 10:00:00").cast("timestamp")) & F.col("method").isNotNull())
    .withColumn("status_upper", F.upper("status"))
)

deduped = (
    base
    .withColumn("rank_in_group", F.row_number().over(w))
    .filter(F.col("rank_in_group") == 1)
    .select("id", "order_id", "amount", "status", "status_upper")
)

enriched = (
    deduped
    .alias("src")
    .join(df_orders.alias("p"), F.col("src.order_id").eqNullSafe(F.col("p.id")), "left")
    .select("src.*", F.col("p.amount").alias("order_amount"))
)

with_windows = (
    enriched
    .select(
        "id",
        "order_id",
        "amount",
        "order_amount",
        F.avg("amount").over(w2).alias("amount_avg_5"),
        F.sum("order_amount").over(w3).alias("cumulative_order_amount"),
        F.avg("order_amount").over(w4).alias("order_amount_avg_6"),
    )
)

cleaned = (
    with_windows
    .filter(F.col("order_id").isNotNull())
    .select(
        "*",
        (F.col("amount") + F.col("amount_avg_5")).alias("amount_plus_amount_avg_5"),
    )
)

result = (
    cleaned
    .select(
        "id",
        "amount",
        "order_amount",
        "amount_avg_5",
        "order_amount_avg_6",
        "amount_plus_amount_avg_5",
    )
    .orderBy(F.col("order_amount_avg_6").desc())
)
