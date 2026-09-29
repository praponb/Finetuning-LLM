from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter((F.col("city") != "Tokyo") & F.upper("supplier_name").like("%URO%"))
    .select("country")
    .orderBy("country")
)
