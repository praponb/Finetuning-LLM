from pyspark.sql import functions as F

result = (
    df_suppliers
    .select(
        "id",
        "country",
        "rating",
        F.when(F.col("city") == "Singapore", 1).otherwise(0).alias("is_singapore"),
        F.trim("country").alias("clean_country"),
    )
)
