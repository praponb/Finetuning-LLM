from pyspark.sql import functions as F

result = (
    df_inventory
    .groupBy("product_id")
    .agg((F.max("quantity") - F.min("quantity")).alias("quantity_range"))
    .filter(F.col("quantity_range") > 21)
    .select("product_id", "quantity_range")
    .orderBy("quantity_range")
)
