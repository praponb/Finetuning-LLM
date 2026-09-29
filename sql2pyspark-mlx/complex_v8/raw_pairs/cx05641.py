from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_category_or_unknown_2 = Window.partitionBy("category_or_unknown_2")
w_is_active_by_weight_kg = Window.partitionBy("is_active").orderBy("weight_kg", "id")

base_skus = (
    df_skus
    .filter(F.col("unit_cost") >= 134.3)
    .select(
        "id",
        "sku_code",
        "category",
        "weight_kg",
        "is_active",
        F.coalesce(F.col("category"), F.lit("UNKNOWN")).alias("category_or_unknown"),
        F.coalesce(F.col("category"), F.lit("UNKNOWN")).alias("category_or_unknown_2"),
    )
)

without_matches = (
    base_skus.alias("s")
    .join(df_stock_moves.alias("st"), (F.col("st.sku_id") == F.col("s.id")) & (F.col("st.move_type").isin("OUTBOUND", "TRANSFER", "INBOUND")), "left_semi")
    .filter(F.col("weight_kg") >= 8.5)
)

sequenced = (
    without_matches
    .select(
        "id",
        "sku_code",
        "category",
        "is_active",
        "category_or_unknown",
        F.round(F.avg("weight_kg").over(w_category_or_unknown_2), 2).alias("category_or_unknown_2_avg_weight_kg"),
        F.row_number().over(w_is_active_by_weight_kg).alias("seq_no"),
        F.round(F.col("weight_kg") - F.avg("weight_kg").over(w_category_or_unknown_2), 2).alias("weight_kg_vs_category_or_unknown_2_avg"),
    )
)

tagged = (
    sequenced
    .select(
        "*",
        F.initcap(F.trim(F.col("category_or_unknown"))).alias("category_or_unknown_clean"),
    )
)

result = (
    tagged
    .filter(F.lower(F.col("category_or_unknown_clean")).like("%le%"))
    .select(
        "id",
        "sku_code",
        "category_or_unknown",
        "category_or_unknown_2_avg_weight_kg",
        "weight_kg_vs_category_or_unknown_2_avg",
        "category_or_unknown_clean",
    )
    .orderBy(
        F.col("category_or_unknown_2_avg_weight_kg").desc(),
        F.col("sku_code").desc(),
    )
)
