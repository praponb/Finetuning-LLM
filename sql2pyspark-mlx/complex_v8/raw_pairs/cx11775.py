from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_category_weight_kg_tier = Window.partitionBy("category", "weight_kg_tier")

cleaned_skus = (
    df_skus
    .filter(F.size(F.col("labels")) >= 2)
    .select(
        "id",
        "sku_code",
        "category",
        "unit_cost",
        "weight_kg",
        "labels",
        F.when(F.col("weight_kg") >= 18.5, "A").when(F.col("weight_kg") >= 12.4, "B").otherwise("C").alias("weight_kg_tier"),
        F.concat_ws(": ", F.col("sku_code"), F.col("category")).alias("sku_code_category_label"),
    )
)

category_stats = (
    cleaned_skus
    .groupBy("category")
    .agg(F.percentile_approx("weight_kg", 0.5).alias("median_weight_kg"))
)

compared = (
    cleaned_skus.alias("s")
    .join(category_stats.alias("g"), F.col("g.category") == F.col("s.category"), "inner")
    .filter(F.col("s.weight_kg") >= F.col("g.median_weight_kg"))
    .select(
        "s.*",
        "g.median_weight_kg",
        F.round(F.col("s.weight_kg") - F.col("g.median_weight_kg"), 2).alias("weight_kg_minus_median"),
    )
)

without_matches = (
    compared.alias("s")
    .join(df_stock_moves.alias("st"), (F.col("st.sku_id") == F.col("s.id")) & (F.col("st.qty") > 91) & (F.col("st.unit_price") < 330.0), "left_anti")
    .filter(F.col("weight_kg") >= 6.4)
)

windowed = (
    without_matches
    .select(
        "*",
        F.min("weight_kg_minus_median").over(w_category_weight_kg_tier).alias("category_weight_kg_tier_min_weight_kg_minus_medi"),
        F.round(F.col("weight_kg") - F.avg("weight_kg").over(w_category_weight_kg_tier), 2).alias("weight_kg_vs_category_weight_kg_tier_avg"),
        F.round(F.col("weight_kg_minus_median") - F.avg("weight_kg_minus_median").over(w_category_weight_kg_tier), 2).alias("weight_kg_minus_median_vs_category_weight_kg_tie"),
    )
)

result = (
    windowed
    .groupBy("category")
    .pivot("weight_kg_tier", ["B", "A", "C"])
    .agg(F.min("weight_kg_vs_category_weight_kg_tier_avg"))
)
