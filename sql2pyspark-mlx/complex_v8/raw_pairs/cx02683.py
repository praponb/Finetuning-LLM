from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_latest = Window.partitionBy("sku_id", "operator").orderBy(F.col("moved_at").desc(), F.col("id").desc())

valid_moves = (
    df_stock_moves
    .filter((F.col("move_type").isNotNull()) & (F.col("qty") >= 152))
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "moved_at",
        "operator",
        "unit_price",
        F.concat(F.col("operator"), F.lit("#"), F.col("warehouse_id").cast("string")).alias("operator_ref"),
    )
)

with_details = (
    valid_moves.alias("s")
    .join(df_skus.alias("sk"), (F.col("sk.id") == F.col("s.sku_id")) & (F.col("sk.weight_kg") > 8.5), "left")
    .select("s.*", F.col("sk.is_active").alias("sku_is_active"))
)

latest = (
    with_details
    .withColumn("row_num", F.row_number().over(w_latest))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

derived = (
    latest
    .withColumn("sku_is_active_label", F.when(F.col("sku_is_active"), "yes").otherwise("no"))
)

filtered = (
    derived
    .crossJoin(derived.agg(F.max("unit_price").alias("overall_max_unit_price")))
    .filter(F.col("unit_price") < F.col("overall_max_unit_price"))
    .drop("overall_max_unit_price")
)

result = (
    filtered
    .groupBy("warehouse_id", "sku_is_active")
    .pivot("operator", ["op_eve", "op_ann"])
    .agg(F.max("qty"))
    .orderBy("warehouse_id", "sku_is_active")
)
