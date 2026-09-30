from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_total_unit_price = Window.orderBy("total_unit_price", "warehouse_id")

prepared = (
    df_stock_moves
    .filter(F.col("moved_at") < F.lit("2025-02-24 12:00:00").cast("timestamp"))
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "unit_price",
        (F.col("moved_at") + F.expr("INTERVAL 1 DAYS")).alias("moved_at_plus_1d"),
        F.when(F.col("move_type").isin("TRANSFER", "ADJUST"), "group_a").when(F.col("move_type") == "INBOUND", "group_b").otherwise("other").alias("move_type_group"),
    )
)

grouped = (
    prepared
    .groupBy("warehouse_id")
    .agg(
        F.round(F.sum("qty") / F.nullif(F.sum("unit_price"), F.lit(0)), 3).alias("qty_to_unit_price_ratio"),
        F.max("unit_price").alias("max_unit_price"),
        F.count(F.when(~F.col("move_type").isin("TRANSFER", "ADJUST"), 1)).alias("n_move_type_not_in_transfer_ad"),
        F.sum(F.when(F.col("move_type") == "ADJUST", F.col("unit_price"))).alias("unit_price_move_type_adjust"),
        F.sum("unit_price").alias("total_unit_price"),
    )
)

sequenced = (
    grouped
    .withColumn("next2_max_unit_price", F.lead("max_unit_price", 2).over(w_all_by_total_unit_price))
    .withColumn("total_unit_price_seq", F.row_number().over(w_all_by_total_unit_price))
)

result = (
    sequenced
    .select(
        "warehouse_id",
        "max_unit_price",
        "n_move_type_not_in_transfer_ad",
        "unit_price_move_type_adjust",
        "total_unit_price",
        "next2_max_unit_price",
        F.least(F.col("qty_to_unit_price_ratio"), F.col("total_unit_price_seq")).alias("least_qty_to_unit_price_ratio_total_unit_price_s"),
        F.round(F.col("total_unit_price") * 1.5, 2).alias("total_unit_price_adj"),
    )
    .orderBy(
        F.col("total_unit_price_adj").desc(),
        F.col("least_qty_to_unit_price_ratio_total_unit_price_s").desc(),
    )
)
