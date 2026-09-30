from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter((F.col("rating") >= 3.9) & F.col("rating").between(3.7, 4.0) & (F.col("rating") > 3.6))
    .select("id", "city")
)
