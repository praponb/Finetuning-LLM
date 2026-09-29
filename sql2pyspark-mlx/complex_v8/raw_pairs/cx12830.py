from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_weight_kg = Window.orderBy(F.col("weight_kg").desc(), "id")
w_all_by_greatest_unit_cost_we = Window.orderBy("greatest_unit_cost_weight_kg", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_greatest_unit_cost_we_2 = Window.orderBy("greatest_unit_cost_weight_kg", "id")

recent = (
    df_skus
    .filter((F.col("category") == "Beauty") & (F.col("sku_code").isNotNull()))
    .select(
        "id",
        "sku_code",
        "unit_cost",
        "weight_kg",
        "labels",
        F.greatest(F.col("unit_cost"), F.col("weight_kg")).alias("greatest_unit_cost_weight_kg"),
    )
)

without_matches = (
    recent.alias("s")
    .join(df_stock_moves.alias("st"), F.col("st.sku_id") == F.col("s.id"), "left_semi")
)

sequenced = (
    without_matches
    .select(
        "*",
        F.row_number().over(w_all_by_weight_kg).alias("rn"),
        F.count(F.lit(1)).over(w_all_by_greatest_unit_cost_we).alias("running_count"),
        F.ntile(5).over(w_all_by_greatest_unit_cost_we_2).alias("greatest_unit_cost_weight_kg_quintile"),
    )
)

result = (
    sequenced
    .groupingSets(
        [["rn", "greatest_unit_cost_weight_kg_quintile"], ["rn"], ["greatest_unit_cost_weight_kg_quintile"]],
        "rn", "greatest_unit_cost_weight_kg_quintile",
    )
    .agg(
        F.grouping("rn").alias("is_total_rn"),
        F.round(100 * F.count(F.when(F.upper(F.trim(F.col("sku_code"))).like("E%"), 1)) / F.count("*"), 1).alias("pct_upper_trim_sku_code_like_e"),
        F.count("*").alias("n_rows"),
        F.percentile_approx("running_count", 0.5).alias("median_running_count"),
    )
    .orderBy(
        F.col("rn").asc_nulls_last(),
        F.col("greatest_unit_cost_weight_kg_quintile").asc_nulls_last(),
    )
)
