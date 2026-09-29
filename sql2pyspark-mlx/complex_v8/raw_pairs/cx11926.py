from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_category_by_weight_kg_prior = Window.partitionBy("category").orderBy(F.col("weight_kg").desc(), "id").rowsBetween(-3, -1)
w_category = Window.partitionBy("category")

filtered = (
    df_skus
    .filter(F.col("weight_kg") <= 21.0)
    .select(
        "id",
        "sku_code",
        "category",
        "unit_cost",
        "weight_kg",
        "labels",
    )
)

category_stats = (
    filtered
    .groupBy("category")
    .agg(F.avg("weight_kg").alias("avg_weight_kg"))
)

above_average = (
    filtered.alias("s")
    .join(category_stats.alias("g"), F.col("g.category") == F.col("s.category"), "inner")
    .filter(F.col("s.weight_kg") > F.col("g.avg_weight_kg"))
    .select(
        "s.*",
        "g.avg_weight_kg",
        F.round(F.col("s.weight_kg") - F.col("g.avg_weight_kg"), 2).alias("weight_kg_minus_avg"),
    )
)

metrics = (
    above_average
    .select(
        "id",
        "sku_code",
        "category",
        "weight_kg",
        "labels",
        "avg_weight_kg",
        "weight_kg_minus_avg",
        F.round(F.avg("unit_cost").over(w_category_by_weight_kg_prior), 2).alias("unit_cost_avg_3_prior"),
        F.min("avg_weight_kg").over(w_category).alias("category_min_avg_weight_kg"),
    )
)

result = (
    metrics
    .select(
        "id",
        "sku_code",
        "category",
        "weight_kg",
        "labels",
        "avg_weight_kg",
        "weight_kg_minus_avg",
        "unit_cost_avg_3_prior",
        F.coalesce(F.col("category"), F.lit("UNKNOWN")).alias("category_or_unknown"),
        F.lower(F.trim(F.col("category"))).alias("category_clean"),
    )
)
