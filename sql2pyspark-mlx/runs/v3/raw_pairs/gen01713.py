from pyspark.sql import functions as F

result = (
    df_payments
    .select("status")
    .unionByName(
        df_shipments
        .filter(F.col("status").isin(["LOST", "RETURNED", "DELIVERED"]))
        .select("status")
    )
    .orderBy("status")
)
