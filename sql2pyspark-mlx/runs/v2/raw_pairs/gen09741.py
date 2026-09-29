from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.col("supplier_name").like("To%") & (F.col("supplier_name").like("Tok%") | F.lower("supplier_name").like("%uro%")))
    .agg(F.count("rating").alias("rating_count"))
)
