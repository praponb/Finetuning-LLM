from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.col("city").isin(["Berlin", "New York", "Chiang Mai"]) | F.col("country").isin(["DE", "JP"]))
    .groupBy("country")
    .agg(
        F.sum(F.when(F.col("country") == "UK", F.col("rating")).otherwise(0)).alias("uk_rating"),
    )
)
