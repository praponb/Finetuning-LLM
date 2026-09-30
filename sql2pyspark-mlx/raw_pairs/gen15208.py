from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("opened_date") <= "2023-06-16")
    .select("status")
    .unionByName(
        df_shipments
        .filter(F.col("carrier").isin(["FedEx", "UPS", "Flash"]))
        .select("status")
    )
)
