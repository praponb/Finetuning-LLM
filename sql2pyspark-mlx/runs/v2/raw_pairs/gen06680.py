from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("product_id").isin([4, 5]) & (F.col("store_id").isin([2, 3, 5, 6]) | (F.col("quantity") > 163)))
)
