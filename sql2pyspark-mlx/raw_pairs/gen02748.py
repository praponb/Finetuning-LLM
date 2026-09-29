from pyspark.sql import functions as F

result = (
    df_suppliers
    .groupBy("country")
    .pivot("city", ["Tokyo", "Singapore"])
    .agg(F.sum("rating").alias("value_sum"), F.count(F.lit(1)).alias("item_count"))
)
