from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_dedup = Window.partitionBy("sku_id", "move_type").orderBy(F.col("qty").desc(), F.col("id").desc())
w_dedup_2 = Window.partitionBy("qty_filled_tier", "sku_id").orderBy(F.col("warehouse_capacity").desc(), F.col("id").desc())

base_moves = (
    df_stock_moves
    .filter((F.col("move_type").isNotNull()) & (F.col("unit_price") <= 313.8))
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "operator",
        F.to_date(F.col("moved_at")).alias("moved_at_date"),
        F.substring(F.trim(F.col("move_type")), 1, 2).alias("move_type_prefix"),
    )
)

latest_per_key = (
    base_moves
    .withColumn("rn", F.row_number().over(w_dedup))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

scored = (
    latest_per_key
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "operator",
        "moved_at_date",
        F.datediff(F.lit("2025-12-31").cast("date"), F.col("moved_at_date")).alias("days_since_moved_at_date"),
        F.coalesce(F.col("qty"), F.lit(0)).alias("qty_filled"),
    )
)

enriched_warehouse = (
    scored.alias("s")
    .join(df_warehouses.alias("w"), F.col("w.id") == F.col("s.warehouse_id"), "left")
    .join(df_skus.alias("sk"), F.col("sk.id") == F.col("s.sku_id"), "left")
    .select(
        "s.id",
        "s.warehouse_id",
        "s.sku_id",
        "s.operator",
        "s.moved_at_date",
        "s.days_since_moved_at_date",
        "s.qty_filled",
        F.col("w.opened_on").alias("warehouse_opened_on"),
        F.col("w.city").alias("warehouse_city"),
        F.col("w.capacity").alias("warehouse_capacity"),
        "sk.sku_code",
    )
)

labeled = (
    enriched_warehouse
    .withColumn("qty_filled_tier", F.when(F.col("qty_filled") >= 350, "large").when(F.col("qty_filled") >= 201, "medium").otherwise("small"))
)

deduped = (
    labeled
    .withColumn("rn", F.row_number().over(w_dedup_2))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

labeled_2 = (
    deduped
    .filter(F.length(F.trim(F.col("sku_code"))) >= 4)
    .select(
        "*",
        F.abs(F.col("days_since_moved_at_date") - 280).alias("days_since_moved_at_date_distance"),
        F.concat(F.col("warehouse_city"), F.lit("#"), F.col("sku_id").cast("string")).alias("warehouse_city_ref"),
        (F.col("warehouse_capacity") + F.col("days_since_moved_at_date")).alias("warehouse_capacity_plus_days_since_moved_at_date"),
    )
)

result = (
    labeled_2
    .filter(F.col("days_since_moved_at_date") > 70)
    .select(
        "id",
        "warehouse_id",
        "operator",
        "moved_at_date",
        "qty_filled",
        "warehouse_city",
        "warehouse_capacity",
        "sku_code",
        "qty_filled_tier",
        "days_since_moved_at_date_distance",
        "warehouse_city_ref",
        "warehouse_capacity_plus_days_since_moved_at_date",
        F.substring(F.trim(F.col("operator")), 1, 2).alias("operator_prefix"),
    )
    .orderBy(F.col("days_since_moved_at_date_distance").desc())
)
