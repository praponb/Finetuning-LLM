from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.col("rating") > 3.8)
    .select(
        "id",
        "rating",
        "city",
        F.round("rating", 1).alias("rating_rounded"),
        F.upper(F.trim("supplier_name")).alias("supplier_name_normalized"),
    )
)
