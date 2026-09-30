from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(~((F.col("city") == "New York") & ((F.col("rating") <= 2.3) | F.col("country").isin(["SG", "DE"]))))
    .select("id", "rating", "city", "country")
)
