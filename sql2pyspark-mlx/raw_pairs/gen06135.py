from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.col("supplier_name").isNotNull() | F.col("supplier_name").isNotNull())
    .select("rating", "country", F.initcap(F.trim("country")).alias("country_title"))
    .orderBy("country_title")
)
