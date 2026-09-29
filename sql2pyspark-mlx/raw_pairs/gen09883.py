from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.col("city").isin(["Tokyo", "Singapore"]))
    .select(
        "region",
        "city",
        F.when(F.col("region") == "East", 1).otherwise(0).alias("is_east"),
    )
    .orderBy(F.col("region").desc(), "city")
)
