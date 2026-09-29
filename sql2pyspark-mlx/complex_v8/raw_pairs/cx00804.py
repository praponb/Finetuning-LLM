from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_latest = Window.partitionBy("sku_id", "move_type").orderBy(F.col("qty").desc(), F.col("id").desc())

scoped_moves = (
    df_stock_moves
    .filter((F.col("qty") < 426) & (F.col("move_type").isNotNull()))
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "moved_at",
        "operator",
        (F.floor(F.col("unit_price") / 100) * 100).alias("unit_price_bucket"),
        F.concat(F.col("move_type"), F.lit("#"), F.col("warehouse_id").cast("string")).alias("move_type_ref"),
    )
)

tagged = (
    scoped_moves
    .select(
        "*",
        F.substring(F.trim(F.col("operator")), 1, 2).alias("operator_prefix"),
        F.coalesce(F.col("unit_price_bucket"), F.lit(0)).alias("unit_price_bucket_filled"),
    )
)

latest_per_key = (
    tagged
    .withColumn("rn", F.row_number().over(w_latest))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

result = (
    latest_per_key
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "qty",
        "moved_at",
        "operator",
        "unit_price_bucket",
        "move_type_ref",
        "unit_price_bucket_filled",
        F.upper(F.trim(F.col("operator"))).alias("operator_clean"),
    )
)
