from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("product_id").isin([3, 4, 9, 10]) & (F.col("quantity") >= 210))
    .select("store_id", F.coalesce("quantity", F.lit(0)).alias("quantity_filled"))
)
