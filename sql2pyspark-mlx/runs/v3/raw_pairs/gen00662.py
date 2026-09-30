from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.col("supplier_name").isNotNull() | F.col("supplier_name").isNotNull())
    .withColumn("supplier_name_slug", F.regexp_replace("supplier_name", " ", ""))
    .withColumn("country_lower", F.lower("country"))
    .orderBy("country_lower")
)
