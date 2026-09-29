from pyspark.sql import functions as F

max_rating = df_suppliers.filter(F.col("supplier_name").like("Pac%")).agg(F.max("rating")).first()[0]

result = (
    df_suppliers
    .filter(F.col("rating") > max_rating)
    .select("id", "rating", "supplier_name")
    .orderBy(F.col("id").desc())
)
