from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("paid_at").cast("bigint")).rangeBetween(-604800, Window.currentRow)
w2 = Window.partitionBy("channel").orderBy("paid_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

cleaned = (
    df_payments
    .filter((F.col("paid_at") > F.lit("2023-09-07 02:00:00").cast("timestamp")) & (~F.col("method").eqNullSafe(F.lit("card"))))
    .withColumn("paid_at_label", F.date_format("paid_at", "MMM yyyy"))
    .withColumn("amount_filled", F.coalesce("amount", F.lit(0)))
)

with_dims = (
    cleaned
    .alias("a")
    .join(df_orders.alias("d"), F.col("a.order_id") == F.col("d.id"), "left")
    .filter(F.col("d.order_date").between("2024-02-01", "2024-12-01"))
    .select(
        "a.id",
        "a.order_id",
        "a.paid_at",
        "a.status",
        "a.paid_at_label",
        "a.amount_filled",
        F.col("d.channel"),
        F.col("d.order_date"),
        F.col("d.amount").alias("order_amount"),
    )
)

cleaned_payments = (
    with_dims
    .filter((F.col("paid_at") > F.to_date(F.lit("2023-08-30"))) & F.col("channel").isNotNull())
    .withColumn("paid_at_date", F.to_date("paid_at"))
)

with_windows = (
    cleaned_payments
    .select(
        "*",
        F.sum("amount_filled").over(w).alias("amount_filled_sum_7d"),
        F.sum("amount_filled").over(w2).alias("cumulative_amount_filled"),
    )
)

result = (
    with_windows
    .alias("e")
    .join(df_orders.alias("o"), F.col("e.order_id") == F.col("o.id"), "left")
    .groupingSets(
        [["order_status", "order_channel"], ["order_status"], []],
        F.col("o.status").alias("order_status"),
        F.col("o.channel").alias("order_channel"),
    )
    .agg(
        F.count("*").alias("cnt"),
        F.min(F.col("e.cumulative_amount_filled")).alias("min_cumulative_amount_filled"),
        F.grouping_id().alias("gid"),
    )
)
