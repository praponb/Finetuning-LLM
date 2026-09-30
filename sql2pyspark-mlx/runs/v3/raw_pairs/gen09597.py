from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter((F.col("supplier_name").like("%Parts%") & (F.col("rating") < 3.1)) | (F.col("rating") <= 2.2))
    .select("id", "country", "rating", F.col("rating").cast("int").alias("rating_int"))
)
