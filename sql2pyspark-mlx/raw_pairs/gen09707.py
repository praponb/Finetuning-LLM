from pyspark.sql import functions as F

min_rating = df_suppliers.filter(F.col("rating") < 3.0).agg(F.min("rating")).first()[0]

result = (
    df_suppliers
    .filter(F.col("rating") <= min_rating)
    .select("id", "rating", "country", "supplier_name")
    .orderBy("country", "rating")
)
