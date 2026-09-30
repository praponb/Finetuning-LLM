from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.col("country") == "UK")
    .select("supplier_name")
    .subtract(df_suppliers.filter(F.col("country") == "JP").select("supplier_name"))
)
