from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_unit_cost_plus_weight = Window.orderBy("unit_cost_plus_weight_kg", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_unit_cost_plus_weight_2 = Window.orderBy("unit_cost_plus_weight_kg", "id")
w_all_by_weight_kg_filled = Window.orderBy(F.col("weight_kg_filled").desc(), "id")
w_all_by_unit_cost = Window.orderBy("unit_cost", "id")

cleaned = (
    df_skus
    .filter(F.upper(F.trim(F.col("sku_code"))).like("A%"))
    .select(
        "id",
        "sku_code",
        "unit_cost",
        "weight_kg",
        "labels",
        F.lower(F.regexp_replace(F.trim(F.col("category")), " ", "_")).alias("category_slug"),
        (F.col("unit_cost") + F.col("weight_kg")).alias("unit_cost_plus_weight_kg"),
        F.coalesce(F.col("weight_kg"), F.lit(0.0)).alias("weight_kg_filled"),
    )
)

screened = (
    cleaned.alias("s")
    .join(df_stock_moves.alias("st"), (F.col("st.sku_id") == F.col("s.id")) & (F.col("st.warehouse_id").isNotNull()), "left_anti")
    .filter(F.length(F.trim(F.col("sku_code"))) >= 6)
)

metrics = (
    screened
    .select(
        "id",
        "sku_code",
        "unit_cost",
        "weight_kg",
        "category_slug",
        "unit_cost_plus_weight_kg",
        "weight_kg_filled",
        F.last("unit_cost_plus_weight_kg", ignorenulls=True).over(w_all_by_unit_cost_plus_weight).alias("last_known_unit_cost_plus_weight_kg"),
        (F.col("unit_cost_plus_weight_kg") - F.lag("unit_cost_plus_weight_kg").over(w_all_by_unit_cost_plus_weight_2)).alias("unit_cost_plus_weight_kg_change"),
        F.ntile(3).over(w_all_by_weight_kg_filled).alias("weight_kg_filled_tercile"),
        F.lag("unit_cost_plus_weight_kg", 2).over(w_all_by_unit_cost).alias("prev2_unit_cost_plus_weight_kg"),
    )
)

qualified = (
    metrics.alias("s")
    .join(df_stock_moves.alias("st"), F.col("st.sku_id") == F.col("s.id"), "left_semi")
)

result = (
    qualified
    .select(
        "id",
        "sku_code",
        "unit_cost",
        "category_slug",
        "unit_cost_plus_weight_kg",
        "weight_kg_filled",
        "unit_cost_plus_weight_kg_change",
        "weight_kg_filled_tercile",
        "prev2_unit_cost_plus_weight_kg",
        F.length(F.trim(F.col("category_slug"))).alias("category_slug_len"),
        F.when(F.col("weight_kg_filled") >= 17.5, "large").when(F.col("weight_kg_filled") >= 7.6, "medium").otherwise("small").alias("weight_kg_filled_tier"),
    )
    .orderBy(F.col("weight_kg_filled").asc())
)
