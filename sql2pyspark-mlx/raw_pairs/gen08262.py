from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(((F.col("product_id") == 8) & F.col("last_restock").between("2023-03-01", "2025-09-26")) | (F.col("last_restock") <= "2025-09-01"))
    .select(
        "id",
        "quantity",
        "last_restock",
        F.date_trunc("MONTH", "last_restock").alias("last_restock_period"),
        F.when(F.col("quantity") >= 229, "High").when(F.col("quantity") >= 117, "Medium").otherwise("Low").alias("quantity_level"),
    )
    .orderBy(F.col("quantity").desc())
)
