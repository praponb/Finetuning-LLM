from pyspark.sql import functions as F

result = (
    df_orders
    .select("status")
    .intersect(
        df_accounts
        .filter(F.col("customer_id").isin([9, 38]))
        .select("status")
    )
    .orderBy("status")
)
