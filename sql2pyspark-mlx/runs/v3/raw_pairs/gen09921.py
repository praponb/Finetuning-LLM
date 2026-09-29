from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.col("city").isin(["London", "Tokyo", "Bangkok"]))
    .select("id", "rating", "city", "supplier_name")
)
