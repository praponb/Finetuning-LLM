from pyspark.sql import functions as F

scoped = df_skus.filter(F.col("category") == "Apparel")

with_activity = (
    scoped.alias("s")
    .join(df_stock_moves.alias("st"), (F.col("st.sku_id") == F.col("s.id")) & (F.col("st.qty") > 138), "left_semi")
)

result = (
    with_activity
    .groupingSets(
        [["is_active", "category"], ["is_active"], ["category"]],
        "is_active", "category",
    )
    .agg(
        F.sum("weight_kg").alias("total_weight_kg"),
        F.round(F.avg("weight_kg"), 2).alias("avg_weight_kg"),
        F.min("weight_kg").alias("min_weight_kg"),
    )
    .orderBy(F.col("is_active").asc_nulls_last(), F.col("category").asc_nulls_last())
)
