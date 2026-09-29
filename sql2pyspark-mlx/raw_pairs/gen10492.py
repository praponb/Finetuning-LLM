from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("product_id").isin([4, 9, 10]) & F.col("product_id").isin([1, 5, 6, 7]))
)
