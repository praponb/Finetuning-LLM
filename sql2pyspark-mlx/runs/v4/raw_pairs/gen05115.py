from pyspark.sql import functions as F

result = (
    df_products
    .groupBy("supplier_id")
    .pivot("category", ["Home", "Electronics"])
    .agg(F.max("cost").alias("value_sum"), F.count(F.lit(1)).alias("item_count"))
)
