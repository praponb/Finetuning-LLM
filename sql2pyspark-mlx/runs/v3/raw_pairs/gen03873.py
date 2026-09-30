from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.col("supplier_name").like("%Parts%") | F.lower("supplier_name").like("%uro%"))
    .withColumn("supplier_name_upper", F.upper("supplier_name"))
    .withColumn("supplier_name_prefix", F.substring("supplier_name", 1, 2))
    .orderBy(F.col("rating").desc())
)
