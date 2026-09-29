from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter((F.col("rating") >= 3.8) & (F.col("country") == "JP"))
    .select("id", "city", F.lower("supplier_name").alias("supplier_name_lower"))
    .orderBy(F.col("city").desc())
)
