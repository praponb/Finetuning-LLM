from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_category_by_unit_cost = Window.partitionBy("category").orderBy(F.col("unit_cost").desc(), "id")
w_is_active = Window.partitionBy("is_active")

base = df_skus.filter(F.lower(F.col("sku_code")).like("%08%"))

annotated = (
    base
    .filter(~F.col("is_active"))
    .withColumn("category_clean", F.lower(F.trim(F.col("category"))))
)

metrics = (
    annotated
    .select(
        "id",
        "sku_code",
        "unit_cost",
        "weight_kg",
        "is_active",
        "category_clean",
        F.lag("weight_kg", 1, 0).over(w_category_by_unit_cost).alias("prev_weight_kg"),
        F.count(F.lit(1)).over(w_is_active).alias("rows_in_is_active"),
    )
)

without_matches = (
    metrics
    .crossJoin(metrics.agg(F.max("unit_cost").alias("overall_max_unit_cost")))
    .filter(F.col("unit_cost") < F.col("overall_max_unit_cost"))
    .drop("overall_max_unit_cost")
)

qualified = (
    without_matches.alias("s")
    .join(df_stock_moves.alias("st"), (F.col("st.sku_id") == F.col("s.id")) & (F.col("st.move_type").isNotNull()), "left_anti")
)

move_stats = (
    df_stock_moves
    .groupBy("sku_id")
    .agg(F.count("*").alias("move_count"))
)

summarized_rows = (
    qualified.alias("s")
    .join(move_stats.alias("st"), F.col("st.sku_id") == F.col("s.id"), "left")
    .select("s.*", F.coalesce(F.col("st.move_count"), F.lit(0)).alias("move_count"))
)

result = (
    summarized_rows
    .select(
        "id",
        "sku_code",
        "unit_cost",
        "weight_kg",
        "is_active",
        "category_clean",
        "prev_weight_kg",
        "rows_in_is_active",
        "move_count",
    )
    .orderBy(F.col("prev_weight_kg").desc())
)
