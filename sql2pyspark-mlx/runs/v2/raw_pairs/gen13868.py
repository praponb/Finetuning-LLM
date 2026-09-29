from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("product_id").isin([6, 7, 8, 9]) & F.month("last_restock").isin([4, 10, 11]) & F.col("last_restock").between("2023-03-01", "2023-09-01"))
    .select("id", "quantity")
)
