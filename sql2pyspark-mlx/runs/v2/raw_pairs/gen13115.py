from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter((F.col("rating") >= 2.4) | (F.col("city") != "London"))
    .select("id", "rating")
)
