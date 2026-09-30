from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.col("rating").between(2.6, 4.0))
    .select(
        "id",
        "supplier_name",
        "city",
        "rating",
        F.floor("rating").alias("rating_whole"),
        F.round((F.col("rating") * 100), 2).alias("rating_adjusted"),
    )
    .orderBy(F.col("rating_whole").desc(), "id")
    .limit(3)
)
