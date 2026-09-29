from pyspark.sql import functions as F

result = (
    df_inventory
    .filter((F.col("quantity") == 159) & (F.col("quantity") < 216) & F.col("product_id").isin([2, 3, 5]))
    .select("id", "product_id", "quantity", "last_restock")
    .orderBy("product_id", "id")
    .limit(20)
)
