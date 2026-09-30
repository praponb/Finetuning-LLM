from pyspark.sql import functions as F

filtered = (
    df_shipments
    .filter((F.col("shipped_date") >= F.lit("2023-11-26").cast("date")) & F.col("carrier").isNotNull())
    .select(
        "*",
        F.last_day("delivered_date").alias("delivered_date_month_end"),
        F.coalesce("carrier", F.lit("DIRECT")).alias("resolved_carrier"),
    )
)

result = (
    filtered
    .groupBy("status", "order_id")
    .pivot("carrier", ["Flash", "FedEx"])
    .agg(F.avg("cost"))
    .select(
        "status",
        "order_id",
        F.col("Flash").alias("flash"),
        F.col("FedEx").alias("fedex"),
    )
)
