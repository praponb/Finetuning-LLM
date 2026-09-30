from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.year("opened_date") >= 2025)
    .select(
        "id",
        "store_name",
        F.round((F.col("sqft") * 0.85), 2).alias("adjusted_sqft"),
        F.when(F.col("region") == "Central", 1).otherwise(0).alias("is_central"),
    )
)
