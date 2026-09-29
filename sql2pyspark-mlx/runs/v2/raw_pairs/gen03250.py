from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("quantity").between(188, 238) & (F.year("last_restock") >= 2024) & F.col("quantity").between(69, 218))
    .select(
        "quantity",
        F.when(F.col("quantity") >= 265, "High").when(F.col("quantity") >= 51, "Medium").otherwise("Low").alias("quantity_level"),
    )
)
