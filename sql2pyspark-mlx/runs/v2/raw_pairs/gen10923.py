from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.col("rating") < 2.9)
    .select("id", "country", F.length("supplier_name").alias("supplier_name_length"))
    .orderBy(F.col("id").desc())
    .limit(10)
)
