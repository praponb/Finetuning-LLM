from pyspark.sql import functions as F

result = (
    df_inventory
    .select("last_restock")
    .subtract(df_accounts.select("opened_date"))
    .orderBy(F.col("last_restock").desc())
)
