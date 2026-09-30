from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.col("city").isin(["London", "Bangkok"]) | (F.col("country") == "US"))
    .select(
        "id",
        "rating",
        F.when(F.col("country") == "SG", 1).otherwise(0).alias("is_sg"),
    )
)
