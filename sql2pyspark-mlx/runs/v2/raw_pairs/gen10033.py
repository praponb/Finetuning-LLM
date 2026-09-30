from pyspark.sql import functions as F

result = (
    df_accounts
    .select("status")
    .intersect(
        df_shipments
        .filter(F.col("order_id").isin([4, 14, 17, 34]))
        .select("status")
    )
)
