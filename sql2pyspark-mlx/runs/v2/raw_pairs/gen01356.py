from pyspark.sql import functions as F

result = (
    df_suppliers
    .select("id", "supplier_name", "city")
    .orderBy(F.col("id").desc())
)
