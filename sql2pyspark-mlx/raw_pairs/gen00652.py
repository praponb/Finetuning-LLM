from pyspark.sql import functions as F

flattened = (
    df_price_tiers
    .select("id", "effective_from", F.explode_outer("discounts").alias("element"))
)

result = (
    flattened
    .withColumn("week_start", F.date_trunc("WEEK", "effective_from"))
    .withColumn("year_start", F.date_trunc("YEAR", "effective_from"))
    .groupingSets(
        [["week_start", "year_start"], ["week_start"]],
        "week_start",
        "year_start",
    )
    .agg(
        F.percentile_approx("element", [0.25, 0.5, 0.75]).alias("element_quartiles"),
        F.count("*").alias("num_rows"),
        F.percentile_approx("element", 0.5).alias("element_p50"),
        F.count("*").alias("row_count"),
        F.countDistinct("id").alias("distinct_ids"),
    )
)
