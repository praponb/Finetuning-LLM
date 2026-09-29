from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter((F.col("rating") >= 3.8) & F.col("country").isin(["JP", "TH", "US"]) & (F.col("rating") > 2.1))
    .select("id", "country", "rating", "city")
)
