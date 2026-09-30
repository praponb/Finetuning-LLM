from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.col("supplier_name").like("E%"))
    .select("city", "country", F.coalesce("rating", F.lit(0)).alias("rating_filled"))
)
