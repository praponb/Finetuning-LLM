from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.col("supplier_name").isNotNull())
    .select("id", "city", "country", "supplier_name")
    .orderBy("country", F.col("id").desc())
)
