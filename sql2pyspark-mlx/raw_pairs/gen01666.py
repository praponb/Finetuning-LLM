from pyspark.sql import functions as F

base = (
    df_shipments
    .filter(F.col("carrier").isin(["Flash", "FedEx", "Kerry"]))
    .select(
        "id",
        "order_id",
        "shipped_date",
        "cost",
        "status",
        F.trim("carrier").alias("clean_carrier"),
        F.coalesce("carrier", F.lit("UNKNOWN")).alias("carrier_filled"),
    )
)

result = (
    base
    .groupBy("order_id")
    .pivot("status", ["LOST", "IN_TRANSIT"])
    .agg(F.sum("cost").alias("total"), F.count(F.lit(1)).alias("cnt"))
)
