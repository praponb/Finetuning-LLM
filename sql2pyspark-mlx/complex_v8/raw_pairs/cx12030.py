from pyspark.sql import functions as F

valid = (
    df_skus
    .filter(F.col("category") == "Toys")
    .select(
        "id",
        "category",
        "unit_cost",
        "labels",
        F.coalesce(F.col("unit_cost"), F.lit(0.0)).alias("unit_cost_filled"),
        F.coalesce(F.col("category"), F.lit("UNKNOWN")).alias("category_or_unknown"),
    )
)

screened = (
    valid.alias("s")
    .join(df_stock_moves.alias("st"), (F.col("st.sku_id") == F.col("s.id")) & (F.col("st.operator").isin("op_ann", "op_bee", "op_eve")) & (F.col("st.unit_price").between(152.7, 293.2)), "left_anti")
    .filter(F.col("category_or_unknown") == "Toys")
)

result = (
    screened
    .groupBy("category_or_unknown")
    .pivot("category", ["Toys", "Food", "Electronics"])
    .agg(F.sum("unit_cost_filled"))
    .orderBy("category_or_unknown")
)
