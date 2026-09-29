from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.col("supplier_name").like("Pa%") | F.col("country").isin(["US", "DE"]))
)
