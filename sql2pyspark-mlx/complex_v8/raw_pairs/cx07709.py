from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_category_category_or_unknown = Window.partitionBy("category", "category_or_unknown")
w_category_category_or_unknown_2 = Window.partitionBy("category", "category_or_unknown").orderBy(F.col("unit_cost").desc(), "id")
w_category_category_or_unknown_3 = Window.partitionBy("category", "category_or_unknown").orderBy(F.col("unit_cost").desc())
w_category_or_unknown_unit_cos = Window.partitionBy("category_or_unknown", "unit_cost_quintile")
w_all_by_unit_cost_per_weight = Window.orderBy(F.col("unit_cost_per_weight_kg").desc(), "id")
w_all_by_unit_cost_per_weight_2 = Window.orderBy(F.col("unit_cost_per_weight_kg").desc(), "id").rowsBetween(-1, Window.currentRow)
w_unit_cost_quintile_category = Window.partitionBy("unit_cost_quintile", "category").orderBy(F.col("weight_kg_share_of_category_category_or_unknown").asc_nulls_last())

valid_skus = (
    df_skus
    .filter(F.col("sku_code").isNotNull())
    .select(
        "id",
        "sku_code",
        "category",
        "unit_cost",
        "weight_kg",
        "labels",
        F.round(F.col("unit_cost") / F.nullif(F.col("weight_kg"), F.lit(0)), 3).alias("unit_cost_per_weight_kg"),
    )
)

with_activity = (
    valid_skus.alias("s")
    .join(df_stock_moves.alias("st"), (F.col("st.sku_id") == F.col("s.id")) & (F.col("st.warehouse_id").isNotNull()) & (F.col("st.unit_price") < 290.4), "left_anti")
    .filter(F.col("weight_kg").between(8.5, 20.9))
)

tagged = (
    with_activity
    .filter(F.col("category").isNotNull())
    .select(
        "*",
        F.coalesce(F.col("category"), F.lit("UNKNOWN")).alias("category_or_unknown"),
        F.concat(F.col("sku_code"), F.lit("#"), F.col("id").cast("string")).alias("sku_code_ref"),
        F.greatest(F.col("unit_cost_per_weight_kg"), F.col("unit_cost")).alias("greatest_unit_cost_per_weight_kg_unit_cost"),
    )
)

metrics = (
    tagged
    .select(
        "*",
        F.min("weight_kg").over(w_category_category_or_unknown).alias("category_category_or_unknown_min_weight_kg"),
        F.round(F.col("weight_kg") / F.nullif(F.sum("weight_kg").over(w_category_category_or_unknown), F.lit(0)), 4).alias("weight_kg_share_of_category_category_or_unknown"),
        F.ntile(5).over(w_category_category_or_unknown_2).alias("unit_cost_quintile"),
        F.round(F.cume_dist().over(w_category_category_or_unknown_3), 3).alias("unit_cost_cume_dist"),
    )
)

labeled = (
    metrics
    .select(
        "*",
        F.concat_ws("|", F.col("sku_code_ref"), F.col("sku_code")).alias("sku_code_ref_sku_code_label"),
    )
)

ranked = (
    labeled
    .select(
        "*",
        F.round(F.avg("unit_cost_per_weight_kg").over(w_category_or_unknown_unit_cos), 2).alias("category_or_unknown_unit_cost_quintile_avg_unit_"),
        F.lead("weight_kg").over(w_all_by_unit_cost_per_weight).alias("next_weight_kg"),
        F.count(F.lit(1)).over(w_category_or_unknown_unit_cos).alias("rows_in_category_or_unknown_unit_cost_quintile"),
        F.round(F.avg("unit_cost_quintile").over(w_category_or_unknown_unit_cos), 2).alias("category_or_unknown_unit_cost_quintile_avg_unit__2"),
        F.sum("unit_cost_cume_dist").over(w_all_by_unit_cost_per_weight_2).alias("unit_cost_cume_dist_sum_2"),
        F.rank().over(w_unit_cost_quintile_category).alias("weight_kg_share_of_category_category_or_unknown_"),
    )
)

result = (
    ranked
    .select(
        "id",
        "next_weight_kg",
        "category_category_or_unknown_min_weight_kg",
        "greatest_unit_cost_per_weight_kg_unit_cost",
    )
    .unpivot(
        ids=["id"],
        values=["next_weight_kg", "category_category_or_unknown_min_weight_kg", "greatest_unit_cost_per_weight_kg_unit_cost"],
        variableColumnName="metric",
        valueColumnName="metric_value",
    )
    .filter(F.col("metric_value").isNotNull())
    .orderBy("id", "metric")
)
