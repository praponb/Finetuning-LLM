from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.col("city").isin(["Singapore", "New York", "Tokyo"]) & (F.col("country").isin(["TH", "JP"]) | (F.col("country") != "UK")))
    .select("id", "supplier_name", "rating", "city")
    .orderBy("supplier_name")
)
