from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.col("country").isin(["JP", "SG", "DE"]) & ~F.col("country").isin(["JP", "UK", "TH"]) & F.col("supplier_name").like("T%"))
    .select("supplier_name", "rating")
)
