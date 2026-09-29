from pyspark.sql import functions as F

valid_shipments = (
    df_shipments
    .filter((F.col("shipped_date") < "2025-08-12") & F.col("carrier").isin(["FedEx", "Flash", "Kerry"]) & (F.col("carrier") != "FedEx"))
)

carrier_summary = (
    valid_shipments
    .groupBy("carrier")
    .agg(F.min("cost").alias("min_cost"))
)

result = carrier_summary.select("carrier", "min_cost")
