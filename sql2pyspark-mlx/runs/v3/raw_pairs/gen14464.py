from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.col("city") != "New York")
    .groupBy("city")
    .agg(
        F.sum(F.when(F.col("city") == "New York", F.col("rating")).otherwise(0)).alias("new_york_rating"),
    )
    .filter(F.col("new_york_rating") < 66)
)
