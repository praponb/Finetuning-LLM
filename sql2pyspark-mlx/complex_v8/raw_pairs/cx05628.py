from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_category_by_weight_kg_per_un = Window.partitionBy("category").orderBy(F.col("weight_kg_per_unit_cost").desc_nulls_first(), "id")
w_category = Window.partitionBy("category")

prepared_skus = (
    df_skus
    .filter(F.col("unit_cost") >= 60.9)
    .select(
        "id",
        "sku_code",
        "category",
        "unit_cost",
        "weight_kg",
        "labels",
        F.concat_ws(" / ", F.col("category"), F.col("sku_code")).alias("category_sku_code_label"),
        F.round(F.col("weight_kg") / F.nullif(F.col("unit_cost"), F.lit(0)), 3).alias("weight_kg_per_unit_cost"),
    )
)

sequenced = (
    prepared_skus
    .withColumn("prev3_weight_kg_per_unit_cost", F.lag("weight_kg_per_unit_cost", 3).over(w_category_by_weight_kg_per_un))
    .withColumn("unit_cost_share_of_category", F.round(F.col("unit_cost") / F.nullif(F.sum("unit_cost").over(w_category), F.lit(0)), 4))
)

with_activity = (
    sequenced.alias("s")
    .join(df_stock_moves.alias("st"), (F.col("st.sku_id") == F.col("s.id")) & (F.to_date(F.col("st.moved_at")) < F.lit("2025-02-24").cast("date")) & (F.col("st.operator").isNotNull()), "left_anti")
)

result = (
    with_activity
    .select(
        "id",
        "sku_code",
        "category",
        "unit_cost",
        "labels",
        "category_sku_code_label",
        "prev3_weight_kg_per_unit_cost",
        "unit_cost_share_of_category",
        F.when(F.col("category").isin("Apparel", "Beauty"), "group_a").when(F.col("category") == "Food", "group_b").otherwise("other").alias("category_group"),
        F.when(F.col("unit_cost") >= 320.4, "HIGH").when(F.col("unit_cost") >= 164.5, "MEDIUM").otherwise("LOW").alias("unit_cost_tier"),
    )
    .orderBy(F.col("category_sku_code_label").desc())
)
