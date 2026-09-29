from pyspark.sql import functions as F

result = (
    df_accounts
    .select("status")
    .unionByName(
        df_shipments
        .filter(F.col("order_id").isin([5, 9, 19, 25]))
        .select("status")
    )
)
