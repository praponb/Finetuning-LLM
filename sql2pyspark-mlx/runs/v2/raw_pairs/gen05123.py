from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(~(F.col("product_id").isin([1, 5, 6, 9]) | (F.col("quantity") == 226)))
)
