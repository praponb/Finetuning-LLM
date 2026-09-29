from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.col("supplier_name").isNotNull() & ((F.col("city") == "Tokyo") | (F.col("rating") < 2.5)))
    .groupBy("city")
    .agg(F.sum("rating").alias("total_rating"))
    .orderBy("city")
    .limit(5)
)
