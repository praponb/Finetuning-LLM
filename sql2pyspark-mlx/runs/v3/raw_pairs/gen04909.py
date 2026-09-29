from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(((F.col("rating") >= 3.7) & (F.col("city") == "Bangkok")) | (F.col("city") == "New York"))
    .select("id", "supplier_name", F.length("city").alias("city_length"))
)
