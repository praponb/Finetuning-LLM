from pyspark.sql import functions as F

result = (
    df_inventory
    .groupBy("product_id", F.year("last_restock").alias("last_restock_year"))
    .agg(F.round(F.stddev("quantity"), 2).alias("stddev_quantity"))
)
