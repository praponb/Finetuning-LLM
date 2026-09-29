from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter((F.col("rating") >= 3.6) & ((F.col("city") != "London") | F.col("supplier_name").like("%Wholesal%")))
)
