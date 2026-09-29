from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_unit_cost_tier = Window.partitionBy("unit_cost_tier")
w_is_active_by_weight_kg_minus = Window.partitionBy("is_active").orderBy(F.col("weight_kg_minus_median_minus_median").desc(), "id")
w_all_by_weight_kg_prior_3 = Window.orderBy("weight_kg", "id").rowsBetween(-3, -1)
w_all_by_weight_kg = Window.orderBy("weight_kg", "id")
w_all_by_weight_kg_full = Window.orderBy("weight_kg", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

valid_skus = (
    df_skus
    .select(
        "id",
        "weight_kg",
        "labels",
        "is_active",
        F.concat_ws(" / ", F.col("sku_code"), F.col("category")).alias("sku_code_category_label"),
        F.when(F.col("unit_cost") >= 251.2, "A").when(F.col("unit_cost") >= 158.7, "B").otherwise("C").alias("unit_cost_tier"),
    )
)

unit_cost_tier_stats = (
    valid_skus
    .groupBy("unit_cost_tier")
    .agg(F.percentile_approx("weight_kg", 0.5).alias("median_weight_kg"))
)

compared = (
    valid_skus.alias("s")
    .join(unit_cost_tier_stats.alias("g"), F.col("g.unit_cost_tier") == F.col("s.unit_cost_tier"), "inner")
    .filter(F.col("s.weight_kg") >= F.col("g.median_weight_kg"))
    .select(
        "s.*",
        "g.median_weight_kg",
        F.round(F.col("s.weight_kg") - F.col("g.median_weight_kg"), 2).alias("weight_kg_minus_median"),
    )
)

unit_cost_tier_stats_2 = (
    compared
    .groupBy("unit_cost_tier")
    .agg(F.percentile_approx("weight_kg_minus_median", 0.5).alias("median_weight_kg_minus_median"))
)

compared_2 = (
    compared.alias("s")
    .join(unit_cost_tier_stats_2.alias("g"), F.col("g.unit_cost_tier") == F.col("s.unit_cost_tier"), "inner")
    .filter(F.col("s.weight_kg_minus_median") >= F.col("g.median_weight_kg_minus_median"))
    .select(
        "s.*",
        "g.median_weight_kg_minus_median",
        F.round(F.col("s.weight_kg_minus_median") - F.col("g.median_weight_kg_minus_median"), 2).alias("weight_kg_minus_median_minus_median"),
    )
)

ranked = (
    compared_2
    .select(
        "*",
        F.round(F.col("median_weight_kg") / F.nullif(F.sum("median_weight_kg").over(w_unit_cost_tier), F.lit(0)), 4).alias("median_weight_kg_share_of_unit_cost_tier"),
        F.round(F.col("weight_kg") - F.avg("weight_kg").over(w_unit_cost_tier), 2).alias("weight_kg_vs_unit_cost_tier_avg"),
        (F.col("weight_kg") - F.lag("weight_kg").over(w_is_active_by_weight_kg_minus)).alias("weight_kg_change"),
        F.max("weight_kg_minus_median").over(w_all_by_weight_kg_prior_3).alias("weight_kg_minus_median_max_3_prior"),
        F.rank().over(w_all_by_weight_kg).alias("weight_kg_rank"),
        F.nth_value("weight_kg_minus_median", 2).over(w_all_by_weight_kg_full).alias("second_weight_kg_minus_median"),
    )
)

line_level_moves = (
    ranked.alias("s")
    .join(df_stock_moves.alias("st"), F.col("st.sku_id") == F.col("s.id"), "inner")
    .select(
        "s.id",
        "s.weight_kg",
        "s.labels",
        "s.is_active",
        "s.median_weight_kg",
        "s.weight_kg_minus_median_minus_median",
        "s.median_weight_kg_share_of_unit_cost_tier",
        "s.weight_kg_change",
        "s.weight_kg_rank",
        "s.second_weight_kg_minus_median",
        F.col("st.id").alias("move_id"),
        F.col("st.qty").alias("move_qty"),
        F.col("st.operator").alias("move_operator"),
    )
)

result = (
    line_level_moves
    .select(
        "id",
        "move_id",
        F.col("second_weight_kg_minus_median").alias("second_weight_kg_minus_median_v"),
        F.col("weight_kg").alias("m_weight_kg"),
        F.col("weight_kg_change").alias("weight_kg_change_v"),
    )
    .unpivot(
        ids=["id", "move_id"],
        values=["second_weight_kg_minus_median_v", "m_weight_kg", "weight_kg_change_v"],
        variableColumnName="metric_name",
        valueColumnName="amount",
    )
    .filter(F.col("amount").isNotNull())
    .orderBy("id", "move_id", "metric_name")
)
