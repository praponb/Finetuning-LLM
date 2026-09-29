from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(((F.col("quantity") <= 201) & (F.col("product_id") == 6)) | F.month("last_restock").isin([8, 10, 11]))
)
