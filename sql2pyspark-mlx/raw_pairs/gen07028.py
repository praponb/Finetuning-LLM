from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter((F.col("country").isin(["UK", "SG", "TH"]) & (F.col("country") != "TH")) | (F.col("country") == "UK"))
    .select("id", "rating", "supplier_name", "country")
)
