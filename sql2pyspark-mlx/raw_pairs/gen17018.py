from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(~F.col("carrier").isin(["UPS", "DHL"]) & ~F.col("carrier").isin(["DHL", "UPS"]))
    .orderBy(F.col("status").desc())
)
