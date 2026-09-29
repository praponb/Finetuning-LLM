from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter((F.col("city") == "Berlin") & (F.col("rating") >= 3.6) & (F.col("city") == "Bangkok"))
    .select("id", "rating")
)
