from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.col("country") == "SG")
    .select(
        "supplier_name",
        "rating",
        "country",
        F.round("rating", 1).alias("rating_rounded"),
    )
    .orderBy("rating_rounded")
)
