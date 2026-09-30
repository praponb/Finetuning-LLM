from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_part = Window.partitionBy("order_id").orderBy("amount", "id")
w2 = Window.partitionBy("order_id").orderBy(F.col("amount").desc(), "id")

cleaned = (
    df_payments
    .filter((F.col("paid_at") >= F.lit("2023-08-28 23:00:00").cast("timestamp")) & F.col("status").isin(["REFUNDED", "FAILED"]))
    .select(
        "id",
        "order_id",
        "amount",
        F.concat("status", F.lit(" - "), "method").alias("status_method"),
    )
)

scored = (
    cleaned
    .select(
        "id",
        "order_id",
        "amount",
        "status_method",
        F.ntile(10).over(w_part).alias("amount_bucket"),
        F.ntile(3).over(w_part).alias("amount_bucket_2"),
        F.row_number().over(w2).alias("event_seq"),
    )
)

kept_keys = (
    scored
    .select("id")
    .intersect(
        df_payments
        .filter(F.col("status").isin(["SUCCESS", "FAILED"]))
        .select("id")
    )
)

kept_rows = (
    scored
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

remaining_keys = (
    kept_rows
    .select("id", "order_id")
    .subtract(
        df_payments
        .filter(F.col("order_id") == 16)
        .select("id", "order_id")
    )
)

kept_rows_payments = (
    kept_rows
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.order_id") == F.col("k.order_id")), "inner")
    .select("s.*")
)

enriched = (
    kept_rows_payments
    .alias("e")
    .join(df_orders.alias("o"), F.col("e.order_id") == F.col("o.id"), "left")
    .select("e.id", "e.order_id", "e.amount", "e.amount_bucket", F.col("o.discount"))
)

result = (
    enriched
    .withColumn("amount_bucket_tier", F.when(F.col("amount_bucket") >= 9, "HIGH").otherwise("LOW"))
    .groupingSets(
        [["amount_bucket_tier", "order_id"], ["amount_bucket_tier"], []],
        "amount_bucket_tier",
        "order_id",
    )
    .agg(
        F.percentile_approx("discount", 0.9).alias("discount_p90"),
        F.countDistinct("id").alias("distinct_ids"),
        F.max("amount").alias("max_amount"),
    )
    .filter(F.col("max_amount") > 0)
)
