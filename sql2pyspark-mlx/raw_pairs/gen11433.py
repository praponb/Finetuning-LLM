from pyspark.sql import functions as F

result = (
    df_inventory
    .select(
        "last_restock",
        "store_id",
        "product_id",
        (F.datediff(F.current_date(), "last_restock") / 7).alias("weeks_since_last_restock"),
    )
    .orderBy("weeks_since_last_restock")
)
