from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy("paid_at_hour", "id")
w2 = Window.partitionBy("status").orderBy(F.col("amount_times_paid_at_hour").asc_nulls_first(), "id")

cleaned = (
    df_payments
    .filter((F.col("paid_at") > F.lit("2023-11-05 08:00:00").cast("timestamp")) & (~F.col("method").eqNullSafe(F.lit("card"))))
    .withColumn("paid_at_hour", F.hour("paid_at"))
    .withColumn("method_filled", F.coalesce("method", F.lit("NONE")))
)

deduped = (
    cleaned
    .withColumn("dedup_rank", F.row_number().over(w))
    .filter(F.col("dedup_rank") == 1)
    .select("id", "amount", "paid_at", "status", "paid_at_hour")
)

filtered = (
    deduped
    .filter((F.col("paid_at") >= F.lit("2023-10-27 03:00:00").cast("timestamp")) & (F.col("amount") > 590))
    .select(
        "*",
        F.round((F.col("paid_at_hour") / F.col("amount")), 2).alias("paid_at_hour_per_amount"),
        (F.col("amount") * F.col("paid_at_hour")).alias("amount_times_paid_at_hour"),
    )
)

latest = (
    filtered
    .withColumn("row_num", F.row_number().over(w2))
    .filter(F.col("row_num") == 1)
)

kept_keys = (
    latest
    .select("id", "status")
    .intersect(
        df_payments
        .filter(F.col("method").isin(["paypal", "promptpay"]))
        .select("id", "status")
    )
)

kept_rows = (
    latest
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.status") == F.col("k.status")), "inner")
    .select("s.*")
)

cleaned_payments = (
    kept_rows
    .filter((F.col("paid_at") >= F.to_date(F.lit("2023-11-03"))) & F.hour("paid_at").between(11, 16))
    .withColumn("amount_times_paid_at_hour_rounded", F.round("amount_times_paid_at_hour", 2))
)

result = (
    cleaned_payments
    .select(
        "id",
        "amount",
        "status",
        "paid_at_hour",
        "amount_times_paid_at_hour",
        "row_num",
    )
    .orderBy("amount")
)
