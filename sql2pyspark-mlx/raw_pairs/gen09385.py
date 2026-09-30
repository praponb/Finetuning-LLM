from pyspark.sql import functions as F

remaining_keys = (
    df_stores
    .select("region", "city")
    .subtract(
        df_stores
        .filter(F.col("sqft") == 12226)
        .select("region", "city")
    )
)

kept_rows = (
    df_stores
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.region") == F.col("k.region")) & (F.col("s.city") == F.col("k.city")), "inner")
    .select("s.*")
)

result = (
    kept_rows
    .withColumn("quarter_start", F.date_trunc("QUARTER", "opened_date"))
    .cube("region", "quarter_start")
    .agg(
        F.round(F.sum("sqft") / F.nullif(F.count(F.when(F.col("region") == "Central", 1)), F.lit(0)), 2).alias("sqft_per_central"),
        F.percentile_approx("sqft", [0.25, 0.5, 0.75]).alias("sqft_quartiles"),
        F.countDistinct("id").alias("distinct_ids"),
        F.grouping("region").alias("grouping_level"),
    )
    .orderBy(F.col("distinct_ids").desc_nulls_last())
)
