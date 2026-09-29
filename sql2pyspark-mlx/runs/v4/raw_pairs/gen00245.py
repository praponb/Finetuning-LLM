from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter((F.col("country") == "JP") & (F.col("country") == "JP"))
    .select("city", "supplier_name", "country")
    .orderBy("supplier_name")
)
