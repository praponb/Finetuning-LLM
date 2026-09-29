from pyspark.sql import functions as F

cleaned = (
    df_suppliers
    .filter(F.col("city").isNotNull())
    .select(
        "*",
        F.round("rating", 1).alias("rating_rounded"),
        F.round("rating", 2).alias("rating_rounded_2"),
    )
)

result = (
    cleaned
    .groupBy("city")
    .pivot("country", ["UK", "US", "TH"])
    .agg(F.sum("rating_rounded_2").alias("total"), F.count(F.lit(1)).alias("n"))
    .select(
        "city",
        F.col("UK_total").alias("uk_total"),
        F.col("UK_n").alias("uk_n"),
        F.col("US_total").alias("us_total"),
        F.col("US_n").alias("us_n"),
        F.col("TH_total").alias("th_total"),
        F.col("TH_n").alias("th_n"),
    )
)
