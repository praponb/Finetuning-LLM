from pyspark.sql import functions as F

result = (
    df_suppliers
    .select("id", "city", "rating", "country")
    .orderBy(F.col("id").desc())
)
