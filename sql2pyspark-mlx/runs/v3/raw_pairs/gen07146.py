from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.upper("supplier_name").like("%IAM%") & F.col("supplier_name").like("%Whol%"))
    .select("id", "rating", F.coalesce("rating", F.lit(0)).alias("rating_filled"))
)
