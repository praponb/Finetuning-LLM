from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter((F.col("country") == "UK") & (F.lower(F.trim("supplier_name")) == "tokyo traders"))
    .groupBy("country")
    .agg(F.sum("rating").alias("total_rating"))
    .filter(F.col("total_rating") > 22)
    .orderBy("total_rating")
)
