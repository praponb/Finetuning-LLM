from pyspark.sql import functions as F

quantity_by_product_id = df_inventory.groupBy("product_id").agg(F.max("quantity").alias("max_inventory_quantity"))

remaining_keys = (
    df_inventory
    .select("store_id", "id")
    .exceptAll(
        df_inventory
        .filter(F.col("last_restock").between("2023-09-01", "2024-08-01"))
        .select("store_id", "id")
    )
)

reconciled = (
    df_inventory
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.store_id") == F.col("k.store_id")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

result = (
    reconciled
    .join(quantity_by_product_id, on="product_id", how="left")
    .select("product_id", "quantity", F.col("max_inventory_quantity"))
    .orderBy(F.col("max_inventory_quantity").desc_nulls_first())
)
