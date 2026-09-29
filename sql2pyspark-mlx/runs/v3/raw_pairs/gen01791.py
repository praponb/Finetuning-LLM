from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.lower("supplier_name").like("%aci%") & F.col("country").isin(["TH", "US"]))
    .select("supplier_name", "rating", "city")
)
