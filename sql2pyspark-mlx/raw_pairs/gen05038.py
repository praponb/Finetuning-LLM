from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy("paid_at", "id")
w2 = Window.partitionBy("order_id").orderBy("paid_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("method").orderBy(F.col("paid_at").desc(), "id")

valid = (
    df_payments
    .filter((F.col("paid_at") > F.lit("2023-07-25 04:00:00").cast("timestamp")) & F.col("method").isNotNull())
    .withColumn("paid_at_label", F.date_format("paid_at", "MMM yyyy"))
)

gaps = (
    valid
    .withColumn("new_group_flag", F.when(F.col("paid_at") - F.lag("paid_at").over(w) > F.expr("INTERVAL 60 DAYS"), 1).otherwise(0))
)

sessionized = (
    gaps
    .select(
        "id",
        "order_id",
        "method",
        "amount",
        "paid_at",
        F.sum("new_group_flag").over(w2).alias("session_id"),
    )
)

latest = (
    sessionized
    .withColumn("rank_in_group", F.row_number().over(w3))
    .filter(F.col("rank_in_group") == 1)
)

valid_payments = (
    latest
    .filter(F.col("order_id").isNotNull() & (F.col("method").eqNullSafe(F.lit("card"))))
    .select(
        "*",
        (F.col("session_id") * F.col("rank_in_group")).alias("session_id_times_rank_in_group"),
        (F.col("session_id") * F.col("rank_in_group")).alias("session_id_times_rank_in_group_2"),
        F.coalesce("method", F.lit("NONE")).alias("resolved_method"),
    )
)

joined = (
    valid_payments
    .alias("src")
    .join(df_orders.alias("p"), F.col("src.order_id").eqNullSafe(F.col("p.id")), "left")
    .filter(~F.col("p.status").isin(["COMPLETED", "SHIPPED", "RETURNED"]))
    .select("src.*", F.col("p.amount").alias("order_amount"), F.col("p.status"))
)

matched_keys = (
    joined
    .select("status", "id")
    .subtract(
        df_payments
        .filter(F.col("order_id").isNotNull())
        .select("status", "id")
    )
)

kept_rows = (
    joined
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.status") == F.col("k.status")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

matched_keys_2 = (
    kept_rows
    .select("status", "order_id")
    .intersect(
        df_payments
        .filter(F.col("status").isin(["FAILED", "SUCCESS"]))
        .select("status", "order_id")
    )
)

verified = (
    kept_rows
    .alias("s")
    .join(matched_keys_2.alias("k"), (F.col("s.status") == F.col("k.status")) & (F.col("s.order_id") == F.col("k.order_id")), "inner")
    .select("s.*")
)

result = (
    verified
    .groupBy("status")
    .pivot("method", ["paypal", "card"])
    .agg(F.sum("order_amount").alias("value_sum"), F.count(F.lit(1)).alias("cnt"))
)
