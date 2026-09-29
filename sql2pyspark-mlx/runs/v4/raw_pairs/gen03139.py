from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.col("country") == "JP")
    .select("country", "city")
    .distinct()
    .orderBy(F.col("city").desc(), "country")
)
