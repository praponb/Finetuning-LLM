from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.col("rating") > 4.1)
    .orderBy(F.col("supplier_name").desc(), "id")
    .limit(3)
)
