from pyspark.sql import functions as F

kept_keys = (
    df_sessions
    .select("country")
    .intersectAll(
        df_sessions
        .filter(~F.col("browser").isin(["Chrome", "Safari"]))
        .select("country")
    )
)

reconciled = (
    df_sessions
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.country") == F.col("k.country")), "inner")
    .select("s.*")
)

result = (
    reconciled
    .withColumn("year_start", F.date_trunc("year", "started_at"))
    .groupingSets(
        [["year_start", "customer_id"], ["year_start"]],
        "year_start",
        "customer_id",
    )
    .agg(
        F.count("*").alias("row_count"),
        F.size(F.collect_set("country")).alias("num_countries"),
        F.countDistinct("id").alias("distinct_ids"),
    )
    .orderBy(F.col("num_countries").desc_nulls_first())
)
