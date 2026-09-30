from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("cost") < 36)
    .select("id", "status", "carrier")
    .intersect(
        df_shipments
        .filter(F.col("status").isin(["DELIVERED", "IN_TRANSIT", "RETURNED"]))
        .select("id", "status", "carrier")
    )
)
