from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(((F.col("country") != "US") & F.col("supplier_name").like("%Par%")) | F.col("supplier_name").like("%Parts%"))
)
