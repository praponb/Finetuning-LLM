from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.col("rating") > 3.7)
    .select("id", "country", "city")
    .orderBy("country", F.col("city").desc(), "id")
    .limit(10)
)
