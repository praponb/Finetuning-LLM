from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_products.filter(F.col("launch_date") > "2023-03-03").alias("p"), F.col("i.product_id") == F.col("p.id"), "left_anti")
    .filter(F.col("i.product_id").isin([1, 3, 7]))
    .select(F.col("i.id"), F.col("i.product_id"))
    .orderBy("id")
)
