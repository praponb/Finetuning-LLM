from pyspark.sql import functions as F

max_rating = df_suppliers.filter(F.col("supplier_name").like("Eu%")).agg(F.max("rating")).first()[0]

result = (
    df_suppliers
    .filter(F.col("rating") > max_rating)
    .select("id", "rating", "city", "country")
)
