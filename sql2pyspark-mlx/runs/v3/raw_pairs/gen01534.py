from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.col("city").isNotNull() & F.col("city").isNotNull())
    .select("rating", "country", "city")
    .orderBy("country", "city")
)
