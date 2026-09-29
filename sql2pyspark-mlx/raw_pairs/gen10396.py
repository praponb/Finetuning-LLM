from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("carrier").isin(["DHL", "Kerry", "Flash"]))
    .select("id", "status")
    .unionByName(
        df_shipments
        .filter(F.col("cost").between(29, 35))
        .select("id", "status")
    )
)
