from pyspark.sql import functions as F

remaining_keys = (
    df_suppliers
    .select("id", "country")
    .intersect(
        df_suppliers
        .filter(F.col("country").isin(["TH", "DE"]))
        .select("id", "country")
    )
)

kept_rows = (
    df_suppliers
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.country") == F.col("k.country")), "inner")
    .select("s.*")
)

result = (
    kept_rows
    .withColumn("rating_tier", F.when(F.col("rating") >= 4.2, "HIGH").otherwise("LOW"))
    .groupingSets(
        [["rating_tier", "country", "city"], ["rating_tier", "country"]],
        "rating_tier",
        "country",
        "city",
    )
    .agg(
        F.count(F.when(F.col("country") == "UK", 1)).alias("uk_count"),
        F.round(F.sum("rating") / F.nullif(F.count(F.when(F.col("country") == "SG", 1)), F.lit(0)), 2).alias("rating_per_sg"),
        F.grouping_id().alias("gid"),
    )
)
