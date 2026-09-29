from pyspark.sql import functions as F

result = (
    df_accounts
    .select("status")
    .unionByName(
        df_shipments
        .filter(F.col("status").isin(["RETURNED", "LOST", "IN_TRANSIT"]))
        .select("status")
    )
)
