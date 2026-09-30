from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("account_type") == "CREDIT")
    .select("opened_date")
    .subtract(
        df_inventory
        .filter(F.col("quantity").between(75, 142))
        .select("last_restock")
    )
)
