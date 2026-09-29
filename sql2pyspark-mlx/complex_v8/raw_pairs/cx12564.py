from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_category_is_active = Window.partitionBy("category", "is_active")
w_is_active_category_or_unknow = Window.partitionBy("is_active", "category_or_unknown").orderBy(F.col("unit_cost").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_is_active_category_or_unknow_2 = Window.partitionBy("is_active", "category_or_unknown").orderBy(F.col("unit_cost").desc(), "id")

base = (
    df_skus
    .filter((F.col("unit_cost") >= 38.3) & (F.col("is_active")))
    .select(
        "id",
        "sku_code",
        "category",
        "unit_cost",
        "weight_kg",
        "is_active",
        F.coalesce(F.col("category"), F.lit("UNKNOWN")).alias("category_or_unknown"),
    )
)

ranked = (
    base
    .select(
        "id",
        "sku_code",
        "category_or_unknown",
        F.round(F.col("weight_kg") - F.avg("weight_kg").over(w_category_is_active), 2).alias("weight_kg_vs_category_is_active_avg"),
        F.first("weight_kg", ignorenulls=True).over(w_is_active_category_or_unknow).alias("first_known_weight_kg"),
        (F.col("unit_cost") - F.lag("unit_cost").over(w_is_active_category_or_unknow_2)).alias("unit_cost_change"),
    )
)

qualified = (
    ranked.alias("s")
    .join(df_stock_moves.alias("st"), (F.col("st.sku_id") == F.col("s.id")) & (F.col("st.qty") > 199), "left_anti")
)

screened = (
    qualified.alias("s")
    .join(df_stock_moves.alias("st"), (F.col("st.sku_id") == F.col("s.id")) & (F.month(F.col("st.moved_at")).isin(2, 3)) & (F.col("st.move_type").isin("OUTBOUND", "INBOUND", "ADJUST")), "left_anti")
)

result = (
    screened
    .filter(F.col("first_known_weight_kg") >= 8.5)
    .select(
        "id",
        "sku_code",
        "category_or_unknown",
        F.length(F.trim(F.col("sku_code"))).alias("sku_code_len"),
    )
)
