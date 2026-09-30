from pyspark.sql import functions as F

result = (
    df_suppliers
    .select("id", "city", "country", "supplier_name")
    .orderBy(F.col("city").desc())
)
