from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.upper("supplier_name").like("%ION%") | (F.col("country") == "TH"))
    .groupBy("country")
    .agg(F.max("rating").alias("max_rating"))
)
