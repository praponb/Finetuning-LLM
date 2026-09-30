from pyspark.sql import functions as F

base = (
    df_orders
    .filter((F.col("order_date") >= F.lit("2023-06-05").cast("date")) & F.col("store_id").isNotNull())
    .select(
        "id",
        "store_id",
        "order_date",
        "status",
        "amount",
        "discount",
        "shipping_fee",
        F.upper("status").alias("status_upper"),
        F.coalesce("channel", F.lit("UNKNOWN")).alias("channel_clean"),
    )
)

result = (
    base
    .alias("src")
    .join(df_stores.alias("p"), F.col("src.store_id") == F.col("p.id"), "inner")
    .groupingSets(
        [["city", "status"], ["city"], []],
        F.col("p.city").alias("city"),
        F.col("src.status").alias("status"),
    )
    .agg(
        F.count("*").alias("cnt"),
        F.min(F.col("src.discount")).alias("min_discount"),
        F.grouping_id().alias("level_id"),
    )
)
