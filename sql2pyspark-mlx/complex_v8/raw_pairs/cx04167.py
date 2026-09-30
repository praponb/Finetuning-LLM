from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_latest = Window.partitionBy("warehouse_id", "move_type").orderBy(F.col("moved_at").desc(), F.col("id").desc())

filtered = (
    df_stock_moves
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "moved_at",
        "operator",
        "unit_price",
    )
)

latest = (
    filtered
    .withColumn("row_num", F.row_number().over(w_latest))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

derived = (
    latest
    .filter(F.col("sku_id") <= 12)
    .withColumn("days_since_moved_at", F.datediff(F.lit("2025-06-30").cast("date"), F.col("moved_at")))
)

with_details = (
    derived.alias("s")
    .join(df_skus.alias("sk"), F.col("sk.id") == F.col("s.sku_id"), "left")
    .select(
        "s.*",
        F.col("sk.weight_kg").alias("sku_weight_kg"),
        F.col("sk.unit_cost").alias("sku_unit_cost"),
        "sk.sku_code",
    )
)

result = (
    with_details
    .groupingSets(
        [["sku_id", "move_type"], ["sku_id"], ["move_type"], []],
        "sku_id", "move_type",
    )
    .agg(
        F.grouping_id().alias("gid"),
        F.round(F.sum("sku_weight_kg"), 2).alias("total_sku_weight_kg"),
        F.round(F.stddev("unit_price"), 2).alias("stddev_unit_price"),
    )
)
