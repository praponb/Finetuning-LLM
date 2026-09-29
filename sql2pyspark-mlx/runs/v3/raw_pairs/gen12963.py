from pyspark.sql import functions as F

result = (
    df_orders
    .select("status")
    .unionByName(
        df_accounts
        .filter(F.col("customer_id").isin([1, 6, 32, 38]))
        .select("status")
    )
)
