from pyspark.sql import functions as F

valid = (
    df_inventory
    .filter(F.col("product_id").isin([2, 6, 8, 9]))
    .select(
        "id",
        "product_id",
        "quantity",
        "last_restock",
        F.add_months("last_restock", 1).alias("last_restock_plus_months"),
        F.round((F.col("quantity") * 1.1), 2).alias("quantity_scaled"),
    )
)

result = (
    valid
    .alias("a")
    .join(df_products.alias("p"), F.col("a.product_id") == F.col("p.id"), "inner")
    .groupBy(F.col("p.brand").alias("brand"))
    .agg(
        F.count("*").alias("rows_total"),
        F.max(F.col("a.quantity_scaled")).alias("max_quantity_scaled"),
        F.max(F.col("a.quantity")).alias("max_quantity"),
    )
    .orderBy(F.col("brand").asc_nulls_first())
)
