from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_prior_rows_12h = Window.partitionBy("prior_rows_12h")
w_prior_rows_12h_by_moved_at = Window.partitionBy("prior_rows_12h").orderBy("moved_at", "id")

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
        F.sum("b.unit_price").alias("prior_unit_price_12h"),
    )
)

metrics = (
    matched
    .select(
        "id",
        "moved_at",
        "prior_unit_price_12h",
        F.size(F.collect_set("sku_id").over(w_prior_rows_12h)).alias("distinct_sku_id_in_prior_rows_12h"),
        F.lag("prior_rows_12h").over(w_prior_rows_12h_by_moved_at).alias("prev_prior_rows_12h"),
    )
)

without_matches = (
    metrics
    .crossJoin(metrics.agg(F.max("prior_unit_price_12h").alias("overall_max_prior_unit_price_12h")))
    .filter(F.col("prior_unit_price_12h") < F.col("overall_max_prior_unit_price_12h"))
    .drop("overall_max_prior_unit_price_12h")
)

tagged = (
    without_matches
    .filter(F.col("moved_at") < F.lit("2025-03-02 12:00:00").cast("timestamp"))
    .select(
        "*",
        F.date_format(F.col("moved_at"), "MMM yyyy").alias("moved_at_label"),
        F.coalesce(F.col("prev_prior_rows_12h"), F.lit(0)).alias("prev_prior_rows_12h_filled"),
    )
)

result = (
    tagged
    .select(
        "id",
        "moved_at",
        "distinct_sku_id_in_prior_rows_12h",
        "prev_prior_rows_12h",
        "prev_prior_rows_12h_filled",
        F.least(F.col("prior_unit_price_12h"), F.col("prev_prior_rows_12h")).alias("least_prior_unit_price_12h_prev_prior_rows_12h"),
        F.datediff(F.lit("2025-06-30").cast("date"), F.col("moved_at")).alias("days_since_moved_at"),
    )
)
