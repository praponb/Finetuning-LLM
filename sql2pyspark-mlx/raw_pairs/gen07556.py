from pyspark.sql import functions as F

valid = (
    df_suppliers
    .filter(~F.col("city").isin(["New York", "Bangkok", "Berlin"]))
    .select(
        "*",
        F.length("country").alias("country_length"),
        F.coalesce("city", F.lit("UNKNOWN")).alias("city_filled"),
    )
)

result = (
    valid
    .groupBy("country")
    .pivot("city", ["New York", "Bangkok"])
    .agg(F.avg("country_length"))
    .select(
        "country",
        F.col("New York").alias("new_york"),
        F.col("Bangkok").alias("bangkok"),
    )
)
