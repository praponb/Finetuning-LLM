from pyspark.sql import functions as F

result = (
    df_stores
    .filter(~F.col("city").isin(["Singapore", "London", "Bangkok"]))
    .select(
        "sqft",
        "city",
        "region",
        F.concat("region", F.lit(" - "), "city").alias("region_city"),
    )
)
