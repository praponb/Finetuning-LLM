from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter((F.col("supplier_name").like("Tok%") & (F.col("country") == "JP")) | (F.col("rating") >= 4.0))
    .select("id", "country", "rating", F.upper("city").alias("city_upper"))
)
