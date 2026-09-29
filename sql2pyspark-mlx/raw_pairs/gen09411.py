from pyspark.sql import functions as F

filtered = (
    df_inventory
    .filter((F.col("last_restock") > F.lit("2023-05-26").cast("date")) & (F.col("last_restock") < "2025-10-30"))
    .select(
        "id",
        "store_id",
        "quantity",
        "last_restock",
        F.add_months("last_restock", 1).alias("last_restock_plus_months"),
    )
)

result = (
    filtered
    .select("store_id", "id")
    .intersectAll(
        df_inventory
        .filter(F.col("quantity") <= 187)
        .select("store_id", "id")
    )
)
