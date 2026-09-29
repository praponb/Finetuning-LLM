from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_warehouse_city_warehouse_ope = Window.partitionBy("warehouse_city", F.year("warehouse_opened_on")).orderBy(F.col("unit_price").desc(), "id").rowsBetween(-2, -1)
w_warehouse_city = Window.partitionBy("warehouse_city")
w_operator_by_warehouse_opened = Window.partitionBy("operator").orderBy("warehouse_opened_on", "id")
w_all_by_unit_price = Window.orderBy("unit_price", "id")
w_warehouse_city_warehouse_ope_2 = Window.partitionBy("warehouse_city", F.year("warehouse_opened_on")).orderBy(F.col("unit_price").desc(), "id")
w_operator_by_warehouse_opened_2 = Window.partitionBy("operator").orderBy("warehouse_opened_on")
w_all_by_unit_price_ties = Window.orderBy("unit_price")

cleaned = (
    df_stock_moves
    .filter(F.col("sku_id") <= 9)
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "qty",
        "operator",
        "unit_price",
        F.greatest(F.col("qty"), F.col("unit_price")).alias("greatest_qty_unit_price"),
    )
)

joined_warehouse = (
    cleaned.alias("s")
    .join(df_warehouses.alias("w"), F.col("w.id") == F.col("s.warehouse_id"), "inner")
    .join(df_skus.alias("sk"), F.col("sk.id") == F.col("s.sku_id"), "left")
    .filter(F.col("w.city") != "Bangkok")
    .select(
        "s.id",
        "s.warehouse_id",
        "s.sku_id",
        "s.operator",
        "s.unit_price",
        F.col("w.opened_on").alias("warehouse_opened_on"),
        F.col("w.capacity").alias("warehouse_capacity"),
        F.col("w.city").alias("warehouse_city"),
        F.col("sk.category").alias("sku_category"),
        "sk.sku_code",
    )
)

ranked = (
    joined_warehouse
    .select(
        "*",
        F.round(F.avg("warehouse_capacity").over(w_warehouse_city_warehouse_ope), 2).alias("warehouse_capacity_avg_2_prior"),
        F.round(F.col("warehouse_capacity") - F.avg("warehouse_capacity").over(w_warehouse_city), 2).alias("warehouse_capacity_vs_warehouse_city_avg"),
        F.round(F.col("unit_price") - F.avg("unit_price").over(w_warehouse_city), 2).alias("unit_price_vs_warehouse_city_avg"),
        F.lead("warehouse_opened_on").over(w_operator_by_warehouse_opened).alias("next_warehouse_opened_on"),
        F.row_number().over(w_all_by_unit_price).alias("unit_price_seq"),
        F.lag("warehouse_capacity").over(w_warehouse_city_warehouse_ope_2).alias("prev_warehouse_capacity"),
        F.dense_rank().over(w_operator_by_warehouse_opened_2).alias("warehouse_opened_on_dense_rank"),
        F.round(F.col("warehouse_capacity") / F.nullif(F.sum("warehouse_capacity").over(w_warehouse_city), F.lit(0)), 4).alias("warehouse_capacity_share_of_warehouse_city"),
        F.round(F.percent_rank().over(w_all_by_unit_price_ties), 3).alias("unit_price_percent_rank"),
        F.dense_rank().over(w_all_by_unit_price).alias("unit_price_dense_rank"),
    )
)

enriched = (
    ranked
    .filter(F.col("prev_warehouse_capacity") > 11005)
    .select(
        "*",
        F.abs(F.col("warehouse_capacity") - 4912).alias("warehouse_capacity_distance"),
        F.round(F.col("unit_price") * 1.07, 2).alias("unit_price_adj"),
        F.abs(F.col("warehouse_capacity") - 11005).alias("warehouse_capacity_distance_2"),
    )
)

result = (
    enriched
    .filter(F.col("unit_price_dense_rank") >= 4)
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "operator",
        "unit_price",
        "warehouse_opened_on",
        "warehouse_capacity",
        "sku_category",
        "sku_code",
        "warehouse_capacity_avg_2_prior",
        "warehouse_capacity_vs_warehouse_city_avg",
        "unit_price_vs_warehouse_city_avg",
        "next_warehouse_opened_on",
        "unit_price_seq",
        "prev_warehouse_capacity",
        "warehouse_opened_on_dense_rank",
        "unit_price_percent_rank",
        "unit_price_dense_rank",
        "warehouse_capacity_distance",
        "warehouse_capacity_distance_2",
        F.when(F.col("unit_price_seq") >= 8, "A").when(F.col("unit_price_seq") >= 5, "B").otherwise("C").alias("unit_price_seq_tier"),
    )
)
