from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.col("supplier_name").isNotNull() | F.lower("supplier_name").like("%ion%"))
    .groupBy("city")
    .agg(F.min("rating").alias("min_rating"))
)
