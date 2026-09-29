from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter((F.col("city") == "Singapore") & (F.col("city") == "London"))
    .select("id", "country", F.upper("city").alias("city_upper"))
)
