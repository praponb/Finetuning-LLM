from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter((F.col("country") == "US") & (F.col("rating") > 4.1))
    .withColumn("clean_country", F.trim("country"))
    .orderBy("supplier_name")
)
