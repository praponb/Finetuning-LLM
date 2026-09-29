from pyspark.sql import functions as F

result = (
    df_inventory
    .filter((F.col("product_id") == 5) & (F.col("quantity") < 99) & F.col("quantity").isNotNull())
    .select("id", "store_id")
)
