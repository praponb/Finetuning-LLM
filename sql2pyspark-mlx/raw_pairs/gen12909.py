from pyspark.sql import functions as F

valid = (
    df_products
    .filter(F.col("launch_date") > F.lit("2023-03-09").cast("date"))
    .withColumn("category_lower", F.lower("category"))
)

result = (
    valid
    .alias("s")
    .join(df_inventory.alias("x"), (F.col("x.product_id") == F.col("s.id")) & (F.col("x.quantity").between(179, 198)), "left_anti")
)
