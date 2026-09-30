from pyspark.sql import functions as F

base = (
    df_shipments
    .filter((F.col("shipped_date") > F.lit("2023-06-16").cast("date")) & F.col("carrier").isNotNull())
    .select(
        "id",
        "order_id",
        "carrier",
        "shipped_date",
        "delivered_date",
        "cost",
        "status",
        F.last_day("delivered_date").alias("delivered_date_month_end"),
        F.col("cost").cast("int").alias("cost_int"),
        F.coalesce("carrier", F.lit("NONE")).alias("resolved_carrier"),
    )
)

result = (
    base
    .groupBy("status")
    .pivot("carrier", ["FedEx", "Flash", "DHL"])
    .agg(F.max("cost").alias("total"))
    .select(
        "status",
        F.col("FedEx").alias("fedex"),
        F.col("Flash").alias("flash"),
        F.col("DHL").alias("dhl"),
    )
)
