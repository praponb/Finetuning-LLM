from pyspark.sql import functions as F

result = (
    df_suppliers
    .select(
        "id",
        "country",
        "rating",
        "supplier_name",
        F.floor("rating").alias("rating_whole"),
        F.round((F.col("rating") * 1.1), 2).alias("rating_adjusted"),
    )
)
