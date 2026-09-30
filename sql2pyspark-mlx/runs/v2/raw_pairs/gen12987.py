from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter((F.col("country") != "JP") & F.col("country").isin(["SG", "UK"]) & (F.col("city") == "Chiang Mai"))
    .select("rating", "supplier_name", "country")
    .orderBy("rating")
)
