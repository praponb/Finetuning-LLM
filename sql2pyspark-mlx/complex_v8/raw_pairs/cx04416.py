from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_valid_from_year_rate_tier = Window.partitionBy(F.year("valid_from"), "rate_tier")
w_all_by_rate_filled_ties = Window.orderBy(F.col("rate_filled").desc())
w_all_by_rate_filled_running = Window.orderBy(F.col("rate_filled").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

filtered = (
    df_tariff_windows
    .filter((F.month(F.col("valid_from")).isin(1, 3)) | (F.col("rate") <= 5.7))
    .select(
        "id",
        "valid_from",
        "valid_to",
        "rate",
    )
)

scored = (
    filtered
    .select(
        "id",
        "valid_from",
        "valid_to",
        F.when(F.col("rate") >= 6.1, "HIGH").when(F.col("rate") >= 3.0, "MEDIUM").otherwise("LOW").alias("rate_tier"),
        F.coalesce(F.col("rate"), F.lit(0.0)).alias("rate_filled"),
        F.round(F.col("rate") * 1.1, 2).alias("rate_adj"),
    )
)

ranked = (
    scored
    .select(
        "*",
        F.max("rate_filled").over(w_valid_from_year_rate_tier).alias("valid_from_year_rate_tier_max_rate_filled"),
        F.round(F.cume_dist().over(w_all_by_rate_filled_ties), 3).alias("rate_filled_cume_dist"),
        F.sum("rate_filled").over(w_all_by_rate_filled_running).alias("running_rate_filled"),
    )
)

filtered_2 = (
    ranked
    .crossJoin(ranked.agg(F.min("rate_filled").alias("overall_min_rate_filled")))
    .filter(F.col("rate_filled") > F.col("overall_min_rate_filled"))
    .drop("overall_min_rate_filled")
)

result = (
    filtered_2
    .filter(F.col("rate_adj") >= 5.0)
    .select(
        "id",
        "valid_from",
        "valid_to",
        "rate_tier",
        "rate_filled",
        "rate_adj",
        "valid_from_year_rate_tier_max_rate_filled",
        "rate_filled_cume_dist",
        "running_rate_filled",
        (F.col("rate_adj") - F.col("rate_filled")).alias("rate_adj_minus_rate_filled"),
    )
    .orderBy(F.col("rate_adj").desc(), "id")
    .limit(20)
)
