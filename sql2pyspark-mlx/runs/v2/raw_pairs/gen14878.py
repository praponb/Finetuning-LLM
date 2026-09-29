from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.col("city") == "Berlin")
    .select("city", "country")
    .distinct()
    .orderBy("country", "city")
    .limit(10)
)
