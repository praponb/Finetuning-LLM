from pyspark.sql import functions as F

result = (
    df_inventory
    .filter((F.col("last_restock") > "2024-10-01") & (F.col("quantity") == 159))
    .select(
        "id",
        "store_id",
        "last_restock",
        F.coalesce("quantity", F.lit(0)).alias("quantity_filled"),
    )
    .orderBy(F.col("quantity_filled").desc(), "id")
    .limit(1)
)
