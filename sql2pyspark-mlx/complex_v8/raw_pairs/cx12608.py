from pyspark.sql import functions as F

matched = (
    df_stock_moves.alias("a")
    .join(
        df_stock_moves.alias("b"),
        (F.col("b.sku_id") == F.col("a.sku_id"))
        & (F.col("b.moved_at") < F.col("a.moved_at"))
        & (F.col("b.moved_at") >= F.col("a.moved_at") - F.expr("INTERVAL 12 HOURS")),
        "left",
    )
    .groupBy("a.id", "a.sku_id", "a.moved_at")
    .agg(
        F.count("b.id").alias("prior_rows_12h"),
        F.sum("b.qty").alias("prior_qty_12h"),
    )
)

enriched = (
    matched
    .filter(F.col("prior_qty_12h") >= 1125)
    .select(
        "id",
        "prior_rows_12h",
        (F.col("prior_qty_12h") + F.col("prior_rows_12h")).alias("prior_qty_12h_plus_prior_rows_12h"),
        F.abs(F.col("prior_rows_12h") - 4).alias("prior_rows_12h_distance"),
        F.coalesce(F.col("prior_qty_12h"), F.lit(0)).alias("prior_qty_12h_filled"),
    )
)

without_matches = (
    enriched
    .crossJoin(enriched.agg(F.avg("prior_qty_12h_plus_prior_rows_12h").alias("overall_avg_prior_qty_12h_plus_prior_rows_12h")))
    .filter(F.col("prior_qty_12h_plus_prior_rows_12h") > F.col("overall_avg_prior_qty_12h_plus_prior_rows_12h"))
    .drop("overall_avg_prior_qty_12h_plus_prior_rows_12h")
)

annotated = (
    without_matches
    .filter(F.col("prior_rows_12h") > 2)
    .withColumn("prior_rows_12h_per_prior_qty_12h_filled", F.round(F.col("prior_rows_12h") / F.nullif(F.col("prior_qty_12h_filled"), F.lit(0)), 3))
)

result = (
    annotated
    .select(
        "id",
        "prior_rows_12h_distance",
        "prior_qty_12h_filled",
        "prior_rows_12h_per_prior_qty_12h_filled",
        F.when(F.col("prior_rows_12h_distance") >= 5, "large").when(F.col("prior_rows_12h_distance") >= 3, "medium").otherwise("small").alias("prior_rows_12h_distance_tier"),
    )
)
