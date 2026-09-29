from pyspark.sql import functions as F

base = (
    df_suppliers
    .filter(F.col("city").isNotNull())
    .withColumn("country_title", F.initcap(F.trim("country")))
    .withColumn("rating_rounded", F.round("rating", 0))
)

result = (
    base
    .groupBy("country")
    .pivot("city", ["Tokyo", "New York"])
    .agg(F.sum("rating_rounded").alias("amount"), F.count(F.lit(1)).alias("n"))
    .select(
        "country",
        F.col("Tokyo_amount").alias("tokyo_amount"),
        F.col("Tokyo_n").alias("tokyo_n"),
        F.col("New York_amount").alias("new_york_amount"),
        F.col("New York_n").alias("new_york_n"),
    )
)
