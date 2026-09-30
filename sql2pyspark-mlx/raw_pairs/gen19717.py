from pyspark.sql import functions as F

result = (
    df_suppliers
    .select("rating", "supplier_name", "country", F.upper("city").alias("city_upper"))
)
