from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter((F.col("rating") >= 2.5) | (F.length("supplier_name") >= 13))
    .orderBy(F.col("id").desc())
)
