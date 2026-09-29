from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(((F.col("country") == "UK") & F.col("supplier_name").isNotNull()) | F.col("rating").between(2.0, 3.3))
    .withColumn("city_lower", F.lower("city"))
    .orderBy("country")
)
