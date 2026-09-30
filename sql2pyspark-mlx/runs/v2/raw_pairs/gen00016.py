from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter((F.col("city") == "Singapore") & F.col("city").isNull() & F.col("supplier_name").like("%Parts%"))
    .select("id", "rating", "supplier_name")
    .orderBy(F.col("id").desc())
)
