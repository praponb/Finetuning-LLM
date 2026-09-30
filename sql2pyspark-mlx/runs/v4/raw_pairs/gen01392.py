from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.col("country").isin(["DE", "US", "UK"]))
    .select(
        "country",
        "rating",
        F.when(F.col("country") == "DE", 1).otherwise(0).alias("is_de"),
    )
)
