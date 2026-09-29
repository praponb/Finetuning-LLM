from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(~F.col("carrier").isin(["Flash", "DHL", "UPS"]))
    .select("order_id")
    .orderBy(F.col("order_id").desc())
)
