from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.col("supplier_name").isNull() & F.col("country").isin(["US", "DE"]))
    .select("rating", "country", "supplier_name")
)
