from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city").orderBy("rating", "id").rowsBetween(-2, Window.currentRow)

filtered = (
    df_suppliers
    .filter(F.col("city").isNotNull())
    .withColumn("supplier_name_country", F.concat("supplier_name", F.lit(", "), "country"))
)

scored = (
    filtered
    .select(
        "*",
        F.min("rating").over(w).alias("last3_min_rating"),
        F.max("rating").over(w).alias("last3_max_rating"),
        F.avg("rating").over(w).alias("last3_avg_rating"),
        F.sum("rating").over(w).alias("last3_sum_rating"),
    )
)

matched_keys = (
    scored
    .select("id")
    .subtract(
        df_suppliers
        .filter(F.lower("supplier_name").like("%ion%"))
        .select("id")
    )
)

kept_rows = (
    scored
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

valid = (
    kept_rows
    .filter(F.col("city").isNotNull())
    .select(
        "*",
        F.round("last3_avg_rating", 1).alias("last3_avg_rating_rounded"),
        F.upper("city").alias("city_upper"),
        F.coalesce("city", F.lit("NONE")).alias("city_filled"),
    )
)

result = (
    valid
    .groupBy("country")
    .pivot("city", ["Singapore", "New York"])
    .agg(
        F.sum("last3_sum_rating").alias("amount"),
        F.count(F.lit(1)).alias("item_count"),
    )
    .select(
        "country",
        F.col("Singapore_amount").alias("singapore_amount"),
        F.col("Singapore_item_count").alias("singapore_item_count"),
        F.col("New York_amount").alias("new_york_amount"),
        F.col("New York_item_count").alias("new_york_item_count"),
    )
)
