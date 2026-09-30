from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("last_restock").between("2024-06-01", "2024-10-05") & F.col("quantity").between(106, 203))
    .select(
        "id",
        "quantity",
        F.when(F.col("quantity") >= 209, "High").when(F.col("quantity") >= 75, "Medium").otherwise("Low").alias("quantity_band"),
    )
    .orderBy("quantity")
)
