from pyspark.sql import functions as F

valid_shipments = (
    df_shipments
    .filter((F.year("shipped_date") == 2025) & F.col("carrier").isin(["Flash", "FedEx"]))
)

carrier_summary = (
    valid_shipments
    .groupBy("carrier")
    .agg(F.sum("cost").alias("total_cost"))
)

result = carrier_summary.select("carrier", "total_cost")
