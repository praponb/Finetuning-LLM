from pyspark.sql import functions as F

result = (
    df_suppliers
    .select("id", "supplier_name", "country", "city")
    .orderBy("id")
    .limit(10)
)
