from pyspark.sql import functions as F

result = (
    df_shipments
    .select("shipped_date")
    .union(
        df_accounts
        .filter(F.col("opened_date") > "2024-12-12")
        .select("opened_date")
    )
    .distinct()
    .orderBy(F.col("shipped_date").desc())
)
