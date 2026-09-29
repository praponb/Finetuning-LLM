from pyspark.sql import functions as F

base = (
    df_orders
    .filter((F.col("order_date") > F.to_date(F.lit("2023-06-14"))) & F.col("store_id").isNotNull())
    .select(
        "*",
        F.length("status").alias("status_length"),
        F.coalesce("channel", F.lit("N/A")).alias("channel_clean"),
    )
)

result = (
    base
    .select("channel", "id")
    .intersect(
        df_orders
        .filter(F.col("order_date").between("2023-03-01", "2025-10-09"))
        .select("channel", "id")
    )
)
