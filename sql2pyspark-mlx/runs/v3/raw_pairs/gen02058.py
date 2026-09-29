from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("quantity").between(77, 104) & F.col("product_id").isin([3, 4, 6, 8]))
)
