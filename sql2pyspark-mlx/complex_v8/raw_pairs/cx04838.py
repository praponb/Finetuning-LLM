from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_latest = Window.partitionBy("sku_id").orderBy("unit_price_adj", "id")
w_latest_2 = Window.partitionBy("move_type").orderBy("qty", "id")

base = (
    df_stock_moves
    .filter((F.col("sku_id") >= 14) | (F.month(F.col("moved_at")).isin(1, 3)))
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "moved_at",
        (F.floor(F.col("qty") / 100) * 100).alias("qty_bucket"),
        F.round(F.col("unit_price") * 1.5, 2).alias("unit_price_adj"),
        F.dayofweek(F.col("moved_at")).alias("moved_at_dayofweek"),
    )
)

latest_per_key = (
    base
    .withColumn("recency_rank", F.row_number().over(w_latest))
    .filter(F.col("recency_rank") == 1)
    .drop("recency_rank")
)

filtered = (
    latest_per_key.alias("s")
    .join(df_stock_moves.alias("st"), F.col("st.sku_id") == F.col("s.sku_id"), "left_semi")
    .filter(F.col("moved_at_dayofweek") <= 6)
)

unique_rows = (
    filtered
    .withColumn("row_num", F.row_number().over(w_latest_2))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

result = (
    unique_rows
    .groupingSets(
        [["move_type", "sku_id"], ["move_type"], ["sku_id"]],
        "move_type", "sku_id",
    )
    .agg(
        F.grouping_id().alias("grouping_level"),
        F.countDistinct("id").alias("distinct_ids"),
        F.round(F.avg("unit_price_adj"), 2).alias("avg_unit_price_adj"),
    )
    .select(
        F.coalesce(F.col("move_type"), F.lit("ALL")).alias("move_type"),
        "sku_id",
        "grouping_level",
        "distinct_ids",
        "avg_unit_price_adj",
    )
)
