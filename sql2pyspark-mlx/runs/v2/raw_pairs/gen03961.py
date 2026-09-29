from pyspark.sql import functions as F

result = (
    df_inventory
    .groupBy(
        "store_id",
        "product_id",
        F.month("last_restock").alias("last_restock_month"),
    )
    .agg(
        F.count("*").alias("cnt"),
        (F.max("quantity") - F.min("quantity")).alias("quantity_range"),
    )
    .orderBy(
        F.col("quantity_range").desc(),
        "store_id",
        "product_id",
        "last_restock_month",
    )
    .limit(50)
)
