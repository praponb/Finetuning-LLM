from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("product_id").isin([5, 6]))
    .select("last_restock")
    .union(
        df_accounts
        .filter(F.col("opened_date") > "2025-02-01")
        .select("opened_date")
    )
    .orderBy(F.col("last_restock").desc())
)
