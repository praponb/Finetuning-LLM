from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("customer_id").isin([9, 20, 26, 27]))
    .select("status")
    .unionByName(
        df_accounts
        .filter(F.col("opened_date") >= "2023-10-18")
        .select("status")
    )
)
