from pyspark.sql import functions as F

result = (
    df_suppliers
    .select("city")
    .distinct()
    .orderBy(F.col("city").desc())
)
