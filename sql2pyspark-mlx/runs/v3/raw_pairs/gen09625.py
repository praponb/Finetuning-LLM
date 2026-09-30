from pyspark.sql import functions as F

result = (
    df_orders
    .select("order_date", F.lit("order").alias("source"))
    .union(
        df_inventory
        .select("last_restock", F.lit("inventory").alias("source"))
    )
)
