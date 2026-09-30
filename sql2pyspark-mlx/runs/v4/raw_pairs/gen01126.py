from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("rating").desc_nulls_last())
w2 = Window.partitionBy("country", "city").orderBy(F.col("rating").desc_nulls_last())

cleaned = (
    df_suppliers
    .filter(F.col("city").isNotNull() & F.col("city").isin(["Bangkok", "Tokyo"]))
    .withColumn("clean_country", F.trim("country"))
    .withColumn("resolved_city", F.coalesce("city", F.lit("DIRECT")))
)

remaining_keys = (
    cleaned
    .select("country", "id")
    .subtract(
        df_suppliers
        .filter(F.col("country").isin(["UK", "DE", "JP"]))
        .select("country", "id")
    )
)

reconciled = (
    cleaned
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.country") == F.col("k.country")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

with_windows = (
    reconciled
    .withColumn("rating_rank", F.rank().over(w))
    .withColumn("rating_cume_dist", F.cume_dist().over(w2))
)

kept = (
    with_windows
    .alias("s")
    .join(df_products.alias("x"), (F.col("x.supplier_id") == F.col("s.id")) & (F.col("x.price") <= 490), "left_anti")
)

totals = (
    kept
    .groupBy("country", "city")
    .agg(
        F.countDistinct("city").alias("distinct_cities"),
        F.max("rating").alias("max_rating"),
        F.round(F.sum("rating") / F.nullif(F.count(F.when(F.col("city") == "Singapore", 1)), F.lit(0)), 2).alias("rating_per_singapore"),
        F.count(F.when(F.col("city") == "Tokyo", 1)).alias("tokyo_rows"),
    )
    .filter(F.col("distinct_cities") < 1)
)

result = (
    totals
    .select("country", "city", "distinct_cities", "rating_per_singapore", "tokyo_rows")
    .orderBy(F.col("country").asc_nulls_last(), F.col("city").desc())
)
