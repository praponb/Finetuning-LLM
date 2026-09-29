from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("shipped_date") < "2025-08-01")
    .groupBy("status")
    .pivot("carrier", ["DHL", "FedEx"])
    .agg(F.max("cost"))
)
