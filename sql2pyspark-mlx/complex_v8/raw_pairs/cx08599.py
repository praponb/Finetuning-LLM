from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_latest = Window.partitionBy("move_type", "warehouse_id").orderBy(F.col("moved_at").desc(), F.col("id").desc())

prepared_moves = (
    df_stock_moves
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "moved_at",
        "unit_price",
        F.date_format(F.col("moved_at"), "MMM yyyy").alias("moved_at_label"),
    )
)

unique_rows = (
    prepared_moves
    .withColumn("row_num", F.row_number().over(w_latest))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

with_details = (
    unique_rows.alias("s")
    .join(df_skus.alias("sk"), F.col("sk.id") == F.col("s.sku_id"), "left")
    .select(
        "s.id",
        "s.warehouse_id",
        "s.sku_id",
        "s.moved_at",
        "s.unit_price",
        "sk.sku_code",
        F.col("sk.labels").alias("sku_labels"),
    )
)

exploded = (
    with_details
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "moved_at",
        "unit_price",
        "sku_code",
        F.explode("sku_labels").alias("sku_label"),
    )
)

labeled = (
    exploded
    .select(
        "*",
        F.when(F.col("sku_label").isin("express", "fragile"), "group_a").when(F.col("sku_label") == "cold", "group_b").otherwise("other").alias("sku_label_group"),
        F.lower(F.trim(F.col("sku_code"))).alias("sku_code_clean"),
    )
)

with_details_warehouse = (
    labeled.alias("s")
    .join(df_warehouses.alias("w"), F.col("w.id") == F.col("s.warehouse_id"), "left")
    .select(
        "s.id",
        "s.warehouse_id",
        "s.sku_id",
        "s.moved_at",
        "s.unit_price",
        "s.sku_code",
        "s.sku_label",
        "s.sku_code_clean",
        F.col("w.wh_code").alias("warehouse_wh_code"),
        F.col("w.city").alias("warehouse_city"),
        F.col("w.capacity").alias("warehouse_capacity"),
    )
)

result = (
    with_details_warehouse
    .select(
        "id",
        "sku_id",
        "moved_at",
        "sku_code",
        "sku_label",
        "sku_code_clean",
        "warehouse_wh_code",
        "warehouse_city",
        "warehouse_capacity",
        F.when(F.col("warehouse_capacity") >= 11506, "HIGH").when(F.col("warehouse_capacity") >= 4912, "MEDIUM").otherwise("LOW").alias("warehouse_capacity_tier"),
        F.datediff(F.lit("2026-01-01").cast("date"), F.col("moved_at")).alias("days_since_moved_at"),
    )
    .orderBy(
        F.col("warehouse_city").desc(),
        F.col("warehouse_capacity").desc_nulls_first(),
        "id",
        "sku_label",
    )
    .limit(20)
)
