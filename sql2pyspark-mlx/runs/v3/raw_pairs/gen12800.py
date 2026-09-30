from pyspark.sql import functions as F

result = (
    df_subscriptions
    .select("start_date")
    .union(df_inventory.select("last_restock"))
    .orderBy("start_date")
)
