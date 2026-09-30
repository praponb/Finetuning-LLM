from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter((F.upper(F.trim("supplier_name")) == "GLOBAL SUPPLY") & (~F.col("country").isin(["TH", "UK"]) | F.col("country").isin(["SG", "DE", "US"])))
    .select("id", "country", "city", F.upper("country").alias("country_upper"))
)
