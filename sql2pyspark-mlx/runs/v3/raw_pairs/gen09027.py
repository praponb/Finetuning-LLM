from pyspark.sql import functions as F

result = (
    df_suppliers
    .select("supplier_name", "city", "rating")
    .orderBy("rating")
)
