from pyspark.sql import functions as F

result = (
    df_suppliers
    .select("id", "supplier_name", F.round("rating", 1).alias("rating_rounded"))
    .orderBy(F.col("id").desc())
    .limit(5)
)
