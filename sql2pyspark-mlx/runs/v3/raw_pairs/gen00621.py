from pyspark.sql import functions as F

result = (
    df_inventory
    .filter((F.year("last_restock") == 2023) & F.month("last_restock").isin([5, 8, 9]) & (F.col("quantity") <= 150))
    .orderBy("quantity", "id")
    .limit(3)
)
