from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_dedup = Window.partitionBy("is_active_label").orderBy(F.col("unit_cost").desc(), F.col("id").desc())

scoped = (
    df_skus
    .filter((~F.col("is_active")) | (F.lower(F.col("sku_code")).like("%08%")))
    .select(
        "id",
        "sku_code",
        "category",
        "unit_cost",
        "weight_kg",
        "labels",
        "is_active",
    )
)

category_stats = (
    scoped
    .groupBy("category")
    .agg(F.percentile_approx("unit_cost", 0.5).alias("median_unit_cost"))
)

vs_group = (
    scoped.alias("s")
    .join(category_stats.alias("g"), F.col("g.category") == F.col("s.category"), "inner")
    .filter(F.col("s.unit_cost") >= F.col("g.median_unit_cost"))
    .select(
        "s.*",
        "g.median_unit_cost",
        F.round(F.col("s.unit_cost") - F.col("g.median_unit_cost"), 2).alias("unit_cost_minus_median"),
    )
)

scored = (
    vs_group
    .select(
        "*",
        (F.col("median_unit_cost") - F.col("unit_cost")).alias("median_unit_cost_minus_unit_cost"),
        F.when(F.col("is_active"), "yes").otherwise("no").alias("is_active_label"),
        F.array_contains(F.col("labels"), "cold").alias("has_cold"),
    )
)

unique_rows = (
    scored
    .withColumn("row_num", F.row_number().over(w_dedup))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

rollup_base = (
    unique_rows
    .groupBy("has_cold", "is_active_label")
    .agg(
        F.max("median_unit_cost").alias("max_median_unit_cost"),
        F.round(F.sum("weight_kg"), 2).alias("total_weight_kg"),
        F.max("unit_cost_minus_median").alias("max_unit_cost_minus_median_check"),
    )
    .filter(F.col("max_unit_cost_minus_median_check") > -120.0)
    .drop("max_unit_cost_minus_median_check")
)

labeled = (
    rollup_base
    .withColumn("max_median_unit_cost_filled", F.coalesce(F.col("max_median_unit_cost"), F.lit(0.0)))
)

result = (
    labeled
    .select(
        "has_cold",
        "is_active_label",
        "max_median_unit_cost",
        "max_median_unit_cost_filled",
        F.when(F.col("max_median_unit_cost") >= 300.2, "large").when(F.col("max_median_unit_cost") >= 100.8, "medium").otherwise("small").alias("max_median_unit_cost_tier"),
        F.coalesce(F.col("max_median_unit_cost"), F.lit(0.0)).alias("max_median_unit_cost_filled_2"),
    )
    .orderBy("max_median_unit_cost", "is_active_label")
)
