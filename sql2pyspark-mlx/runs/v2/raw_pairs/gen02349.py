from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.col("region").isNotNull())
    .select(
        "id",
        "region",
        F.when(F.col("region") == "Central", 1).otherwise(0).alias("is_central"),
    )
)
