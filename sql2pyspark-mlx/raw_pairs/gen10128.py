from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter((F.col("country").isin(["TH", "DE", "UK"]) & F.col("city").isin(["Singapore", "New York", "Berlin"])) | (F.col("city") == "Berlin"))
    .select("city", "supplier_name")
)
