from pyspark.sql import functions as F

valid = (
    df_inventory
    .filter((F.col("last_restock") > F.lit("2023-05-20").cast("date")) & F.col("store_id").isin([1, 2]))
    .select(
        "id",
        "product_id",
        "last_restock",
        F.col("quantity").cast("string").alias("quantity_str"),
        F.date_add("last_restock", 71).alias("last_restock_plus_71d"),
    )
)

result = (
    valid
    .select("product_id", "id")
    .subtract(
        df_inventory
        .filter(F.col("quantity") > 195)
        .select("product_id", "id")
    )
)
