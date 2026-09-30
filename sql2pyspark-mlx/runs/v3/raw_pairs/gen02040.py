from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.col("supplier_name").isNull())
    .select("id", "rating", "city", "country")
    .orderBy(F.col("id").desc())
)
