from pyspark.sql import functions as F

result = (
    df_products
    .alias("s")
    .join(df_inventory.alias("x"), (F.col("x.product_id") == F.col("s.id")) & (F.col("x.last_restock") >= F.col("s.launch_date")), "left_semi")
)
