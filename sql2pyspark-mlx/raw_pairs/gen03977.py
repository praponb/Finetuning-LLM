from pyspark.sql import functions as F

cleaned = (
    df_inventory
    .filter(F.col("store_id") == 6)
    .select(
        "id",
        "product_id",
        "quantity",
        "last_restock",
        F.when(F.col("quantity") >= 201, "High").when(F.col("quantity") >= 104, "Medium").otherwise("Low").alias("quantity_band"),
        (F.datediff(F.current_date(), "last_restock") / 30).alias("months_since_last_restock"),
    )
)

result = (
    cleaned
    .select("product_id", "id")
    .intersectAll(
        df_inventory
        .filter(F.col("product_id").isin([5, 6, 8]))
        .select("product_id", "id")
    )
)
