from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("age") < 58)
    .select(
        "segment",
        "age",
        "is_active",
        F.when(F.col("country") == "UK", 1).otherwise(0).alias("is_uk"),
    )
    .orderBy("segment")
)
