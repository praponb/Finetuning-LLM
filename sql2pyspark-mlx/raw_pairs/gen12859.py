from pyspark.sql import functions as F

result = (
    df_accounts
    .select("opened_date")
    .union(
        df_shipments
        .filter(F.col("status") == "DELIVERED")
        .select("shipped_date")
    )
    .orderBy("opened_date")
)
