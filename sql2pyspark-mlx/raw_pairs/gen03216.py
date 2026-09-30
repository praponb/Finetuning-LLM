from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.col("supplier_name").like("P%"))
    .select(
        "rating",
        "country",
        "supplier_name",
        F.ceil("rating").alias("rating_whole"),
    )
    .orderBy(F.col("rating").desc())
)
