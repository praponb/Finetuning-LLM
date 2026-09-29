from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.col("city").isin(["Tokyo", "New York"]))
    .groupBy("country")
    .agg(
        F.sum(F.when(F.col("country") == "UK", F.col("rating")).otherwise(0)).alias("uk_rating"),
    )
    .filter(F.col("uk_rating") > 25)
)
