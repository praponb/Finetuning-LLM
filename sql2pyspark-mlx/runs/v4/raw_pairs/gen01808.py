from pyspark.sql import functions as F

result = (
    df_suppliers
    .select(
        "id",
        "country",
        "rating",
        "city",
        F.col("rating").cast("int").alias("rating_int"),
    )
)
