from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy(F.col("delivered_date").desc(), "id")
w2 = Window.partitionBy("carrier")
w3 = Window.orderBy(F.col("is_lost").desc_nulls_first())
w4 = Window.partitionBy("order_id").orderBy("days_since_shipped_date")
w5 = Window.partitionBy(F.month("shipped_date"), "order_id")
w6 = Window.partitionBy(F.date_trunc("month", "shipped_date"), "order_id").orderBy(F.col("is_lost").desc())

cleaned = (
    df_shipments
    .filter((F.col("carrier").eqNullSafe(F.lit("FedEx"))) & (F.col("shipped_date") >= F.to_date(F.lit("2023-02-11"))))
    .select(
        "id",
        "order_id",
        "carrier",
        "shipped_date",
        "delivered_date",
        "status",
        F.datediff(F.current_date(), "shipped_date").alias("days_since_shipped_date"),
        F.coalesce("carrier", F.lit("DIRECT")).alias("carrier_filled"),
    )
)

latest = (
    cleaned
    .withColumn("dedup_rank", F.row_number().over(w))
    .filter(F.col("dedup_rank") == 1)
)

remaining_keys = (
    latest
    .select("order_id")
    .exceptAll(
        df_shipments
        .filter(F.col("shipped_date") >= "2025-05-01")
        .select("order_id")
    )
)

verified = (
    latest
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.order_id") == F.col("k.order_id")), "inner")
    .select("s.*")
)

base = (
    verified
    .filter((F.col("shipped_date") >= F.lit("2023-04-06").cast("date")) & F.col("carrier").isNotNull())
    .select(
        "*",
        F.datediff(F.current_date(), "shipped_date").alias("days_since_shipped_date_2"),
        F.when(F.col("status") == "LOST", 1).otherwise(0).alias("is_lost"),
    )
)

joined = (
    base
    .alias("src")
    .join(df_orders.alias("p"), F.col("src.order_id") == F.col("p.id"), "left")
    .select("src.*", F.col("p.amount"), F.col("p.status").alias("order_status"))
)

metrics = (
    joined
    .select(
        "order_id",
        "shipped_date",
        "carrier_filled",
        "days_since_shipped_date_2",
        "is_lost",
        "amount",
        F.round((F.col("amount") / F.sum("amount").over(w2)), 4).alias("amount_share"),
        F.dense_rank().over(w3).alias("is_lost_dense_rank"),
        F.cume_dist().over(w4).alias("days_since_shipped_date_cume_dist"),
        F.count("is_lost").over(w5).alias("shipped_date_month_count_is_lost"),
        F.percent_rank().over(w6).alias("is_lost_percent_rank"),
    )
)

result = (
    metrics
    .groupBy("order_id")
    .agg(
        F.percentile_approx("amount_share", 0.9).alias("amount_share_p90"),
        F.count("*").alias("num_rows"),
        F.percentile_approx("is_lost_percent_rank", [0.25, 0.5, 0.75]).alias("is_lost_percent_rank_quartiles"),
    )
    .orderBy(F.col("num_rows").desc())
)
