from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter((F.col("country") == "US") & (F.col("country") != "JP"))
    .withColumn("supplier_name_lower", F.lower("supplier_name"))
    .withColumn("supplier_name_slug", F.regexp_replace("supplier_name", " ", "-"))
)
