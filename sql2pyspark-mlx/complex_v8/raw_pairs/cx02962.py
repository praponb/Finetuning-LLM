from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_dedup = Window.partitionBy("warehouse_id", "sku_id").orderBy(F.col("warehouse_capacity").desc(), F.col("id").desc())

cleaned = (
    df_stock_moves
    .filter((F.col("sku_id") >= 17) & (F.col("qty") >= 168))
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "qty",
        "operator",
        "unit_price",
    )
)

joined = (
    cleaned.alias("s")
    .join(df_skus.alias("sk"), F.col("sk.id") == F.col("s.sku_id"), "inner")
    .select(
        "s.id",
        "s.warehouse_id",
        "s.sku_id",
        "s.qty",
        "s.operator",
        "s.unit_price",
        F.col("sk.labels").alias("sku_labels"),
    )
)

derived = joined.withColumn("qty_bucket", F.floor(F.col("qty") / 100) * 100)

joined_2 = (
    derived.alias("s")
    .join(df_warehouses.alias("w"), F.col("w.id") == F.col("s.warehouse_id"), "left")
    .join(df_skus.alias("sk"), F.col("sk.id") == F.col("s.sku_id"), "left")
    .select(
        "s.id",
        "s.warehouse_id",
        "s.sku_id",
        "s.operator",
        "s.sku_labels",
        "s.qty_bucket",
        F.col("w.opened_on").alias("warehouse_opened_on"),
        F.col("w.city").alias("warehouse_city"),
        F.col("w.capacity").alias("warehouse_capacity"),
        "sk.sku_code",
    )
)

without_matches = (
    joined_2.alias("s")
    .join(df_warehouses.alias("w"), (F.col("w.id") == F.col("s.warehouse_id")) & (F.col("w.capacity") <= 11768) & (~F.col("w.city").isin("Bangkok", "Singapore")), "left_semi")
)

latest = (
    without_matches
    .withColumn("recency_rank", F.row_number().over(w_dedup))
    .filter(F.col("recency_rank") == 1)
    .drop("recency_rank")
)

result = (
    latest
    .select(
        "id",
        "sku_id",
        "sku_labels",
        "qty_bucket",
        "warehouse_opened_on",
        "warehouse_city",
        "warehouse_capacity",
        "sku_code",
        F.lower(F.regexp_replace(F.trim(F.col("sku_code")), " ", "_")).alias("sku_code_slug"),
    )
)
