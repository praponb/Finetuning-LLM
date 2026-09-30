from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id")
w2 = Window.orderBy("revenue", "id")
w3 = Window.orderBy(F.col("units_share").desc_nulls_first())
w4 = Window.partitionBy("seq_no").orderBy(F.col("seq_no_p95").desc())

cleaned = (
    df_store_sales
    .filter(F.col("store_id").isin([2, 4, 5]))
    .select(
        "id",
        "store_id",
        "product_id",
        "units",
        "revenue",
        (F.col("revenue") - F.col("units")).alias("revenue_per_unit"),
    )
)

scored = (
    cleaned
    .select(
        "*",
        F.round((F.col("units") / F.sum("units").over(w)), 4).alias("units_share"),
        F.row_number().over(w2).alias("seq_no"),
    )
)

valid = (
    scored
    .filter(F.col("seq_no") > 3)
    .withColumn("units_share_filled", F.coalesce("units_share", F.lit(0)))
)

remaining_keys = (
    valid
    .select("id")
    .subtract(
        df_store_sales
        .filter(F.col("revenue").between(2000, 3400))
        .select("id")
    )
)

kept_rows = (
    valid
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

scored_store_sales = (
    kept_rows
    .select(
        "id",
        "product_id",
        "revenue",
        "revenue_per_unit",
        "units_share",
        "seq_no",
        F.percent_rank().over(w3).alias("units_share_percent_rank"),
    )
)

base = (
    scored_store_sales
    .filter(F.col("units_share_percent_rank") >= 0.51)
    .select(
        "*",
        F.round((F.col("units_share") * 0.85), 2).alias("adjusted_units_share"),
        (F.col("revenue_per_unit") + F.col("revenue")).alias("revenue_per_unit_plus_revenue"),
    )
)

cleaned_store_sales = (
    base
    .filter(F.col("units_share_percent_rank") <= 0.47)
    .select(
        "product_id",
        "revenue",
        "seq_no",
        "revenue_per_unit_plus_revenue",
        (F.col("seq_no") * F.col("revenue_per_unit_plus_revenue")).alias("seq_no_times_revenue_per_unit_plus_revenue"),
    )
)

summary = (
    cleaned_store_sales
    .groupBy(
        "seq_no",
        F.when(F.col("revenue") >= 3800, "LARGE").otherwise("SMALL").alias("revenue_tier"),
    )
    .agg(
        F.percentile_approx("seq_no", [0.25, 0.5, 0.75]).alias("seq_no_quartiles"),
        F.percentile_approx("seq_no", [0.25, 0.5, 0.75]).alias("seq_no_quartiles_2"),
        F.percentile_approx("seq_no", 0.95).alias("seq_no_p95"),
    )
    .withColumn("seq_no_p95_percent_rank", F.percent_rank().over(w4))
)

result = (
    summary
    .select(
        "seq_no",
        "revenue_tier",
        "seq_no_quartiles",
        "seq_no_quartiles_2",
        "seq_no_p95",
        "seq_no_p95_percent_rank",
    )
    .orderBy(F.col("seq_no_p95_percent_rank").desc_nulls_last())
)
