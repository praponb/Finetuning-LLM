from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("quantity").between(112, 198))
    .select("id", "store_id", "quantity")
)
