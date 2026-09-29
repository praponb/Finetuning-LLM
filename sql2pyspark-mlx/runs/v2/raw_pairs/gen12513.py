from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.col("city").isin(["Berlin", "Chiang Mai"]))
    .select("id", "supplier_name", F.col("rating").cast("int").alias("rating_int"))
)
