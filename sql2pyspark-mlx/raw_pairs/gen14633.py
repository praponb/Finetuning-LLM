from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.col("supplier_name").like("Gl%") & (F.col("rating") < 2.4))
    .select("rating")
)
