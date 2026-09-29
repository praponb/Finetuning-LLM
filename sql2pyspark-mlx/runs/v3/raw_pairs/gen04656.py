from pyspark.sql import functions as F

result = (
    df_suppliers
    .withColumn("supplier_name_length", F.length("supplier_name"))
    .withColumn("clean_supplier_name", F.trim("supplier_name"))
    .orderBy("supplier_name_length")
)
