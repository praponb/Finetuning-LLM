from pyspark.sql import functions as F

result = (
    df_orders
    .filter(~F.col("status").isin(["COMPLETED", "CANCELLED"]))
    .select("status")
    .unionByName(df_shipments.filter(F.col("carrier") != "FedEx").select("status"))
    .distinct()
    .orderBy(F.col("status").desc())
)
