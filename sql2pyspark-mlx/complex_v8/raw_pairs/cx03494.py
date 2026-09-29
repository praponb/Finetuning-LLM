from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_latest = Window.partitionBy("warehouse_id").orderBy(F.col("moved_at_label_len").desc(), F.col("id").desc())

valid = (
    df_stock_moves
    .filter(F.col("unit_price") > 119.5)
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "moved_at",
        "unit_price",
        F.date_format(F.col("moved_at"), "yyyy-MM").alias("moved_at_label"),
        (F.floor(F.col("unit_price") / 100) * 100).alias("unit_price_bucket"),
    )
)

annotated = (
    valid
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "unit_price",
        "moved_at_label",
        F.length(F.trim(F.col("moved_at_label"))).alias("moved_at_label_len"),
        F.substring(F.trim(F.col("move_type")), 1, 3).alias("move_type_prefix"),
    )
)

latest_per_key = (
    annotated
    .withColumn("rn", F.row_number().over(w_latest))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

with_details_sku = (
    latest_per_key.alias("s")
    .join(df_skus.alias("sk"), F.col("sk.id") == F.col("s.sku_id"), "inner")
    .join(df_warehouses.alias("w"), F.col("w.id") == F.col("s.warehouse_id"), "left")
    .select(
        "s.*",
        F.col("sk.weight_kg").alias("sku_weight_kg"),
        F.col("sk.is_active").alias("sku_is_active"),
        "sk.sku_code",
        F.col("w.capacity").alias("warehouse_capacity"),
    )
)

result = (
    with_details_sku
    .select("id", "unit_price", "sku_weight_kg")
    .unpivot(
        ids=["id"],
        values=["unit_price", "sku_weight_kg"],
        variableColumnName="metric",
        valueColumnName="reading",
    )
    .filter(F.col("reading").isNotNull())
)
