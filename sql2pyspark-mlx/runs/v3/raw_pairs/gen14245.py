from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.col("city") != "London")
    .withColumn("city_length", F.length("city"))
    .orderBy("supplier_name")
)
