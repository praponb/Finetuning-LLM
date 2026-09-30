from pyspark.sql import functions as F

filtered = (
    df_inventory
    .filter(F.col("last_restock") >= F.lit("2023-05-20").cast("date"))
    .select(
        "id",
        "store_id",
        "quantity",
        "last_restock",
        F.when(F.col("quantity") >= 240, "High").when(F.col("quantity") >= 50, "Medium").otherwise("Low").alias("quantity_tier"),
        F.col("quantity").cast("double").alias("quantity_num"),
    )
)

result = (
    filtered
    .select("id", "store_id")
    .subtract(
        df_inventory
        .filter(F.year("last_restock") >= 2023)
        .select("id", "store_id")
    )
)
