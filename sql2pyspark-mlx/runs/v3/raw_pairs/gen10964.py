from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("age") >= 41)
    .select(
        "id",
        "country",
        F.when(F.col("country") == "SG", 1).otherwise(0).alias("is_sg"),
    )
)
