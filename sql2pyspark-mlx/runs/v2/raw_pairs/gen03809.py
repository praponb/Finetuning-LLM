from pyspark.sql import functions as F

result = (
    df_inventory
    .select(
        "store_id",
        "product_id",
        F.last_day("last_restock").alias("last_restock_month_end"),
        F.coalesce("quantity", F.lit(0)).alias("quantity_filled"),
    )
    .orderBy(F.col("store_id").desc(), "product_id")
)
