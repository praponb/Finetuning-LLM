from pyspark.sql import functions as F

avg_rating = df_suppliers.filter(F.col("city") != "Chiang Mai").agg(F.avg("rating")).first()[0]

result = (
    df_suppliers
    .filter(F.col("rating") >= avg_rating)
    .select("id", "rating")
    .orderBy(F.col("id").desc())
)
