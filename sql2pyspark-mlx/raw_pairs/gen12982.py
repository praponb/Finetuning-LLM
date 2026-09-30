from pyspark.sql import functions as F

matched_keys = (
    df_suppliers
    .select("id")
    .subtract(
        df_suppliers
        .filter(F.col("country").isin(["TH", "JP", "SG"]))
        .select("id")
    )
)

reconciled = (
    df_suppliers
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

result = (
    reconciled
    .groupBy(
        F.when(F.col("rating") >= 3.6, "LARGE").otherwise("SMALL").alias("rating_tier"),
    )
    .agg(
        F.round(F.avg("rating"), 2).alias("avg_rating"),
        F.count_if(F.col("city") == "London").alias("london_events"),
    )
    .filter(F.col("london_events") >= 1)
    .orderBy("rating_tier")
)
