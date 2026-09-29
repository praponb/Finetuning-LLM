from pyspark.sql import functions as F

max_rating = df_suppliers.filter(F.col("rating").between(2.9, 4.1)).agg(F.max("rating")).first()[0]

result = (
    df_suppliers
    .filter(F.col("rating") > max_rating)
    .select("id", "rating")
    .orderBy(F.col("id").desc())
)
