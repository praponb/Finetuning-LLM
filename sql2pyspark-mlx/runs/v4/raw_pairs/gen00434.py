from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("quantity").between(101, 120) & ((F.col("quantity") >= 219) | F.col("product_id").isin([3, 6])))
    .select("id", "product_id", "quantity")
    .orderBy(F.col("quantity").desc())
)
