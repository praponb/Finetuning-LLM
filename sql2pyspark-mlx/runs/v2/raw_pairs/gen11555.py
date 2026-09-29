from pyspark.sql import functions as F

result = (
    df_inventory
    .groupBy(
        F.date_trunc("MONTH", "last_restock").alias("last_restock_month"),
        "product_id",
        "store_id",
    )
    .agg(
        F.max("quantity").alias("max_quantity"),
        F.count("quantity").alias("quantity_count"),
    )
    .orderBy(
        F.col("store_id").desc(),
        "max_quantity",
        "last_restock_month",
        "product_id",
    )
    .limit(10)
)
