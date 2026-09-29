from pyspark.sql import functions as F

with_dims = (
    df_store_sales
    .alias("s")
    .join(df_stores.alias("p2"), F.col("s.store_id") == F.col("p2.id"), "left")
    .select("s.id", "s.store_id", "s.product_id", "s.units", F.col("p2.opened_date"))
)

result = (
    with_dims
    .withColumn("week_start", F.date_trunc("week", "opened_date"))
    .groupingSets(
        [["week_start", "product_id"], ["week_start"]],
        "week_start",
        "product_id",
    )
    .agg(
        F.round(F.avg("units"), 2).alias("avg_units"),
        F.countDistinct("id").alias("distinct_ids"),
        F.countDistinct("id").alias("distinct_ids_2"),
        F.percentile_approx("units", 0.95).alias("units_p95"),
        F.grouping("week_start").alias("grouping_id"),
    )
    .filter(F.col("units_p95") > 0)
    .orderBy(F.col("product_id").desc())
)
