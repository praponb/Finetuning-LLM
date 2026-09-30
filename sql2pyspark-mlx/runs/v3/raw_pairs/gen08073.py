from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.col("city").isin(["New York", "Tokyo"]) & F.col("supplier_name").like("Eur%"))
    .select("city", F.trim("supplier_name").alias("clean_supplier_name"))
)
