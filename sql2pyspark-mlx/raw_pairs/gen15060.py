from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.col("city").isNull())
    .select("city", "supplier_name")
)
