from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(((F.col("rating") <= 2.6) & (F.length("supplier_name") < 13)) | F.col("city").isin(["Bangkok", "Berlin", "New York"]))
    .orderBy(F.col("id").desc())
)
