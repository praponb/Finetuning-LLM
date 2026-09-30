from pyspark.sql import functions as F

avg_rating = df_suppliers.filter(F.col("city") != "Berlin").agg(F.avg("rating")).first()[0]

result = (
    df_suppliers
    .filter(F.col("rating") >= avg_rating)
    .select("id", "rating", "city", "supplier_name")
    .orderBy(F.col("rating").desc())
)
