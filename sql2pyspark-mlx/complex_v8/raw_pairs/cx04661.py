from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_latest = Window.partitionBy("category").orderBy(F.col("unit_cost_adj").desc(), F.col("id").desc())

filtered = (
    df_skus
    .filter(F.col("category").isin("Food", "Apparel"))
    .select(
        "id",
        "sku_code",
        "category",
        "unit_cost",
        "weight_kg",
        "labels",
        "is_active",
        F.round(F.col("unit_cost") / F.nullif(F.col("weight_kg"), F.lit(0)), 3).alias("unit_cost_per_weight_kg"),
        F.round(F.col("unit_cost") * 1.5, 2).alias("unit_cost_adj"),
        (F.floor(F.col("weight_kg") / 5) * 5).alias("weight_kg_bucket"),
    )
)

latest_per_key = (
    filtered
    .withColumn("row_num", F.row_number().over(w_latest))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

qualified = (
    latest_per_key.alias("s")
    .join(df_stock_moves.alias("st"), (F.col("st.sku_id") == F.col("s.id")) & (F.col("st.warehouse_id").isNotNull()), "left_semi")
)

result = (
    qualified
    .select(
        "id",
        "unit_cost_adj",
        "weight_kg",
        "unit_cost",
    )
    .unpivot(
        ids=["id"],
        values=["unit_cost_adj", "weight_kg", "unit_cost"],
        variableColumnName="measure",
        valueColumnName="reading",
    )
)
