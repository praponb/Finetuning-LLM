from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.col("rating").between(1.9, 3.5))
    .select("city")
    .distinct()
)
