from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_latest = Window.partitionBy("category").orderBy(F.col("unit_cost").desc(), F.col("id").desc())

filtered = (
    df_skus
    .filter((F.col("is_active")) & (F.col("weight_kg") > 8.5))
    .select(
        "id",
        "sku_code",
        "category",
        "unit_cost",
        "weight_kg",
        "labels",
        "is_active",
        F.concat_ws(" / ", F.col("sku_code"), F.col("category")).alias("sku_code_category_label"),
    )
)

qualified = (
    filtered.alias("s")
    .join(df_stock_moves.alias("st"), (F.col("st.sku_id") == F.col("s.id")) & (F.col("st.warehouse_id").isNotNull()) & (F.col("st.unit_price") <= 363.6), "left_anti")
    .filter(F.col("sku_code_category_label").isNotNull())
)

deduped = (
    qualified
    .withColumn("row_num", F.row_number().over(w_latest))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

result = (
    deduped
    .select("id")
    .distinct()
    .orderBy("id")
)
