from pyspark.sql import functions as F

result = (
    df_inventory
    .groupBy(
        "product_id",
        F.date_format("last_restock", "yyyy-MM").alias("last_restock_month"),
    )
    .agg(
        (F.max("quantity") - F.min("quantity")).alias("quantity_range"),
        F.count("*").alias("cnt"),
    )
)
