from pyspark.sql import functions as F

result = (
    df_suppliers
    .withColumn("is_berlin", F.when(F.col("city") == "Berlin", 1).otherwise(0))
    .withColumn("country_supplier_name_key", F.concat_ws("-", "country", "supplier_name"))
    .orderBy("country_supplier_name_key")
)
