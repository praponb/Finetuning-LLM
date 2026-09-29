from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("status").isin(["SHIPPED", "CANCELLED", "PENDING"]))
    .select("status")
    .unionByName(
        df_accounts
        .filter(F.col("opened_date") >= "2023-11-01")
        .select("status")
    )
)
