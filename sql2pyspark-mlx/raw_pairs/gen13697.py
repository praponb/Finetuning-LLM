from pyspark.sql import functions as F

result = (
    df_payments
    .select("status")
    .unionByName(
        df_shipments
        .filter(F.col("carrier").isin(["Kerry", "DHL"]))
        .select("status")
    )
)
