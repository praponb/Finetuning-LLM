from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_latest = Window.partitionBy("unit_cost_tier").orderBy(F.col("unit_cost").desc(), F.col("id").desc())
w_dedup = Window.partitionBy("category", "unit_cost_tier").orderBy(F.col("avg_weight_kg").desc(), F.col("id").desc())

cleaned = (
    df_skus
    .filter((F.col("category").isin("Beauty", "Electronics")) & (F.col("unit_cost") > 68.0))
    .select(
        "id",
        "sku_code",
        "category",
        "unit_cost",
        "weight_kg",
        "is_active",
        F.round(F.col("weight_kg") * 1.5, 2).alias("weight_kg_adj"),
        F.when(F.col("unit_cost") >= 302.5, "large").when(F.col("unit_cost") >= 134.3, "medium").otherwise("small").alias("unit_cost_tier"),
    )
)

deduped = (
    cleaned
    .withColumn("recency_rank", F.row_number().over(w_latest))
    .filter(F.col("recency_rank") == 1)
    .drop("recency_rank")
)

category_stats = (
    deduped
    .groupBy("category")
    .agg(F.avg("weight_kg").alias("avg_weight_kg"))
)

outliers = (
    deduped.alias("s")
    .join(category_stats.alias("g"), F.col("g.category") == F.col("s.category"), "inner")
    .filter(F.col("s.weight_kg") > F.col("g.avg_weight_kg"))
    .select(
        "s.*",
        "g.avg_weight_kg",
        F.round(F.col("s.weight_kg") - F.col("g.avg_weight_kg"), 2).alias("weight_kg_minus_avg"),
    )
)

latest = (
    outliers
    .withColumn("row_num", F.row_number().over(w_dedup))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

result = (
    latest
    .cube("is_active", "category", "unit_cost_tier")
    .agg(F.count(F.when(F.col("is_active"), 1)).alias("n_is_active_true"))
    .select(
        "is_active",
        F.coalesce(F.col("category"), F.lit("ALL")).alias("category"),
        F.coalesce(F.col("unit_cost_tier"), F.lit("ALL")).alias("unit_cost_tier"),
        "n_is_active_true",
    )
)
