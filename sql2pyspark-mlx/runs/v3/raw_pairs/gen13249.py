from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter((F.col("city") == "Singapore") & (F.col("city") == "London"))
    .select(
        "rating",
        F.col("rating").cast("int").alias("rating_int"),
        F.coalesce("city", F.lit("Unknown")).alias("city_filled"),
    )
)
