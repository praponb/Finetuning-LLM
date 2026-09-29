from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.length("supplier_name") > 8)
    .withColumn("rating_rounded", F.round("rating", 1))
    .withColumn("country_length", F.length("country"))
    .orderBy(F.col("supplier_name").desc(), "id")
    .limit(1)
)
