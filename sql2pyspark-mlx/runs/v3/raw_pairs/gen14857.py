from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.col("rating").between(3.0, 4.1))
    .select("id", "city")
)
