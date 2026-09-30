from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.col("supplier_name").isNotNull())
    .withColumn("supplier_name_prefix", F.substring("supplier_name", 1, 3))
    .orderBy(F.col("rating").desc())
)
