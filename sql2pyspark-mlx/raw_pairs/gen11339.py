from pyspark.sql import functions as F

cleaned = (
    df_shipments
    .filter((F.col("carrier") == "Flash") & (F.col("shipped_date") >= F.lit("2023-09-18").cast("date")))
    .select(
        "*",
        F.date_format("shipped_date", "yyyy-MM-dd").alias("shipped_date_label"),
    )
)

valid = (
    cleaned
    .filter(F.col("delivered_date") >= F.lit("2023-04-02").cast("date"))
    .select(
        "id",
        "order_id",
        "shipped_date",
        "delivered_date",
        "cost",
        "status",
        "shipped_date_label",
        F.lower("status").alias("status_lower"),
        F.datediff(F.current_date(), "shipped_date").alias("days_since_shipped_date"),
    )
)

enriched = (
    valid
    .alias("src")
    .join(df_orders.alias("o"), F.col("src.order_id") == F.col("o.id"), "left")
    .filter(F.col("o.store_id") == 10)
    .select("src.*", F.col("o.discount"), F.col("o.channel"), F.col("o.order_date"))
)

joined = (
    enriched
    .alias("src")
    .join(df_orders.alias("b"), F.col("src.order_id") == F.col("b.id"), "left")
    .select(
        "src.id",
        "src.order_id",
        "src.delivered_date",
        "src.days_since_shipped_date",
        "src.discount",
        F.col("b.discount").alias("order_discount"),
        F.col("b.order_date").alias("order_order_date"),
    )
)

grouped = (
    joined
    .groupBy(
        F.date_trunc("quarter", "order_order_date").alias("quarter_start"),
        F.when(F.col("days_since_shipped_date") >= 66, "PREMIUM").otherwise("STANDARD").alias("days_since_shipped_date_tier"),
    )
    .agg(
        F.percentile_approx("days_since_shipped_date", 0.95).alias("days_since_shipped_date_p95"),
        F.count("*").alias("row_count"),
        F.percentile_approx("discount", 0.95).alias("discount_p95"),
        F.percentile_approx("discount", [0.25, 0.5, 0.75]).alias("discount_quartiles"),
        F.percentile_approx("order_discount", 0.9).alias("order_discount_p90"),
    )
)

result = (
    grouped
    .select(
        "quarter_start",
        "days_since_shipped_date_tier",
        "days_since_shipped_date_p95",
        "row_count",
        "discount_p95",
        "order_discount_p90",
    )
)
