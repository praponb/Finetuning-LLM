from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.col("country").isin(["TH", "SG", "UK"]))
    .select("supplier_name", F.upper("country").alias("country_upper"))
)
