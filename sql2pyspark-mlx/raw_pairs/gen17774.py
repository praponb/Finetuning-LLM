from pyspark.sql import functions as F

result = (
    df_suppliers
    .groupBy("country")
    .pivot("city", ["Bangkok", "London", "Berlin"])
    .agg(F.avg("rating").alias("value_sum"), F.count(F.lit(1)).alias("n"))
    .select(
        "country",
        F.col("Bangkok_value_sum").alias("bangkok_value_sum"),
        F.col("Bangkok_n").alias("bangkok_n"),
        F.col("London_value_sum").alias("london_value_sum"),
        F.col("London_n").alias("london_n"),
        F.col("Berlin_value_sum").alias("berlin_value_sum"),
        F.col("Berlin_n").alias("berlin_n"),
    )
)
