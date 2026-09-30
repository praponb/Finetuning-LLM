from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.col("city") == "New York")
    .select("id", "country", "supplier_name", F.upper("country").alias("country_upper"))
    .orderBy("country_upper", "id")
    .limit(5)
)
