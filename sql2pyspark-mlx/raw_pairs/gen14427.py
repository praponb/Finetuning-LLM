from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter((F.col("country") != "DE") & (F.col("city").isin(["Berlin", "London", "New York"]) | (F.col("country") != "UK")))
)
