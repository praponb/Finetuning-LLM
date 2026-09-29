from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("rating", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("city").orderBy("rating", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("city").orderBy(F.col("first_known_rating_minus_cumulative_rating").desc_nulls_first(), "id")

valid = (
    df_suppliers
    .filter(F.col("country") == "DE")
    .select(
        "id",
        "city",
        "rating",
        F.upper("city").alias("city_upper"),
        F.when(F.col("country").isin(["UK", "US"]), "Core").otherwise("Other").alias("country_group"),
        F.coalesce("city", F.lit("N/A")).alias("resolved_city"),
    )
)

metrics = (
    valid
    .select(
        "id",
        "city",
        "city_upper",
        "resolved_city",
        F.first("rating", ignorenulls=True).over(w).alias("first_known_rating"),
        F.sum("rating").over(w2).alias("cumulative_rating"),
        F.sum("rating").over(w2).alias("running_rating"),
    )
)

valid_suppliers = (
    metrics
    .filter(F.col("city").isNotNull())
    .select(
        "id",
        "city",
        "city_upper",
        "first_known_rating",
        "cumulative_rating",
        "running_rating",
        (F.col("first_known_rating") - F.col("cumulative_rating")).alias("first_known_rating_minus_cumulative_rating"),
        F.coalesce("city", F.lit("UNKNOWN")).alias("city_filled"),
    )
)

ranked = (
    valid_suppliers
    .withColumn("row_num", F.row_number().over(w3))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

result = (
    ranked
    .withColumn("first_known_rating_minus_cumulative_rating_tier", F.when(F.col("first_known_rating_minus_cumulative_rating") >= 84, "HIGH").otherwise("LOW"))
    .rollup("city", "first_known_rating_minus_cumulative_rating_tier")
    .agg(
        F.sum(F.when(F.col("city") == "London", F.col("first_known_rating_minus_cumulative_rating")).otherwise(0)).alias("london_first_known_rating_minus_cumulative_rating"),
        F.count("*").alias("row_count"),
        F.round(F.sum("first_known_rating_minus_cumulative_rating") / F.nullif(F.count(F.when(F.col("city") == "Bangkok", 1)), F.lit(0)), 2).alias("first_known_rating_minus_cumulative_rating_per_bangkok"),
        F.sum(F.when(F.col("city") == "New York", F.col("first_known_rating_minus_cumulative_rating")).otherwise(0)).alias("new_york_first_known_rating_minus_cumulative_rating"),
        F.percentile_approx("first_known_rating_minus_cumulative_rating", 0.95).alias("first_known_rating_minus_cumulative_rating_p95"),
        F.sum(F.when(F.col("city") == "Berlin", F.col("cumulative_rating")).otherwise(0)).alias("berlin_cumulative_rating"),
        F.grouping("city").alias("grouping_level"),
    )
    .orderBy(F.col("city").desc())
)
