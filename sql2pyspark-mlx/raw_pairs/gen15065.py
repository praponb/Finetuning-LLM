from pyspark.sql import functions as F

result = (
    df_inventory
    .select("last_restock")
    .intersect(df_accounts.select("opened_date"))
    .orderBy("last_restock")
)
