from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.col("city").isin(["New York", "Tokyo"]))
    .select(
        "id",
        "supplier_name",
        "rating",
        "country",
        F.floor("rating").alias("rating_whole"),
        F.round("rating", 1).alias("rating_rounded"),
    )
    .orderBy(F.col("supplier_name").desc())
)
