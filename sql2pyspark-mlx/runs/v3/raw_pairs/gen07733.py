from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.col("supplier_name").like("L%") & ((F.lower(F.trim("supplier_name")) == "siam parts co") | F.col("supplier_name").like("Glo%")))
)
