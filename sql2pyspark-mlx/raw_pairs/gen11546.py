from pyspark.sql import functions as F

result = (
    df_suppliers
    .select("city", "country")
    .distinct()
    .orderBy("country")
)
