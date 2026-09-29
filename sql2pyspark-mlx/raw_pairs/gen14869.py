from pyspark.sql import functions as F

base = (
    df_shipments
    .filter(F.col("carrier").isNotNull() & (~F.col("carrier").eqNullSafe(F.lit("UPS"))))
    .select(
        "id",
        "order_id",
        "carrier",
        "shipped_date",
        "delivered_date",
        "cost",
        F.add_months("shipped_date", 12).alias("shipped_date_plus_months"),
        F.coalesce("carrier", F.lit("NONE")).alias("carrier_clean"),
    )
)

result = (
    base
    .groupBy("order_id")
    .pivot("carrier", ["FedEx", "Kerry"])
    .agg(F.avg("cost").alias("value_sum"))
)
