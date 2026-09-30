from pyspark.sql import functions as F

base = df_inventory.filter(F.col("store_id") == 4)

last_restock_year_summary = (
    base
    .groupBy(F.year("last_restock").alias("last_restock_year"), "product_id")
    .agg(
        F.round(F.stddev("quantity"), 2).alias("stddev_quantity"),
        F.max("quantity").alias("max_quantity"),
    )
)

result = (
    last_restock_year_summary
    .filter(F.col("max_quantity") < 180)
    .select("last_restock_year", "product_id", "stddev_quantity", "max_quantity")
)
