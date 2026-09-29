from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.col("supplier_name").like("%Global%") & F.col("supplier_name").like("Eu%"))
    .withColumn("city_length", F.length("city"))
    .withColumn("supplier_name_upper", F.upper("supplier_name"))
)
