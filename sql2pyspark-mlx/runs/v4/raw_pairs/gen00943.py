from pyspark.sql import functions as F

filtered = (
    df_shipments
    .filter((F.col("delivered_date") >= F.lit("2023-12-03").cast("date")) & (F.col("shipped_date") <= "2024-04-01"))
    .select(
        "id",
        "order_id",
        "carrier",
        "shipped_date",
        "cost",
        "status",
        F.date_add("delivered_date", 14).alias("delivered_date_plus_14d"),
        F.coalesce("carrier", F.lit("UNKNOWN")).alias("carrier_clean"),
    )
)

result = (
    filtered
    .groupBy(F.date_trunc("quarter", "delivered_date_plus_14d").alias("quarter_start"))
    .agg(
        F.size(F.collect_set("status")).alias("num_status"),
        F.max("cost").alias("max_cost"),
    )
)
