from pyspark.sql import functions as F

result = (
    df_accounts
    .select("status")
    .unionByName(
        df_shipments
        .filter(F.col("delivered_date") < "2024-09-01")
        .select("status")
    )
    .distinct()
)
