from pyspark.sql import functions as F

valid = (
    df_skus
    .filter(~F.col("is_active"))
    .select(
        "id",
        "sku_code",
        "weight_kg",
        "labels",
        "is_active",
    )
)

screened = (
    valid.alias("s")
    .join(df_stock_moves.alias("st"), (F.col("st.sku_id") == F.col("s.id")) & (F.col("st.operator").isin("op_bee", "op_chan", "op_dew")) & (F.month(F.col("st.moved_at")).isin(1, 3)), "left_anti")
)

result = (
    screened
    .select(
        "id",
        "sku_code",
        "weight_kg",
        "is_active",
        F.coalesce(F.col("weight_kg"), F.lit(0.0)).alias("weight_kg_filled"),
    )
    .orderBy(F.col("weight_kg_filled").desc())
)
