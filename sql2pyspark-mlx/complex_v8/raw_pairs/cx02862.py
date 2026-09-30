from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_operator_warehouse_id_by_uni = Window.partitionBy("operator", "warehouse_id").orderBy(F.col("unit_price_minus_qty_adj").desc(), "id")
w_operator_unit_price_minus_qt = Window.partitionBy("operator", "unit_price_minus_qty_tier")
w_operator_warehouse_id_by_uni_2 = Window.partitionBy("operator", "warehouse_id").orderBy(F.col("unit_price_minus_qty_adj").desc())

prepared = (
    df_stock_moves
    .filter(F.col("sku_id") <= 7)
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "operator",
        "unit_price",
        F.lower(F.trim(F.col("operator"))).alias("operator_clean"),
        (F.col("unit_price") - F.col("qty")).alias("unit_price_minus_qty"),
        (F.floor(F.col("unit_price") / 100) * 100).alias("unit_price_bucket"),
    )
)

tagged = (
    prepared
    .select(
        "id",
        "warehouse_id",
        "move_type",
        "operator",
        "unit_price",
        "operator_clean",
        "unit_price_minus_qty",
        "unit_price_bucket",
        F.when(F.col("unit_price_minus_qty") >= 850.1, "large").when(F.col("unit_price_minus_qty") >= 400.6, "medium").otherwise("small").alias("unit_price_minus_qty_tier"),
        F.upper(F.trim(F.col("operator"))).alias("operator_clean_2"),
        F.round(F.col("unit_price_minus_qty") * 1.07, 2).alias("unit_price_minus_qty_adj"),
    )
)

tagged_2 = (
    tagged
    .filter(F.col("unit_price") >= 112.8)
    .select(
        "id",
        "warehouse_id",
        "operator",
        "unit_price",
        "operator_clean",
        "unit_price_minus_qty",
        "unit_price_bucket",
        "unit_price_minus_qty_tier",
        "operator_clean_2",
        "unit_price_minus_qty_adj",
        F.substring(F.trim(F.col("move_type")), 1, 3).alias("move_type_prefix"),
        F.upper(F.trim(F.col("move_type"))).alias("move_type_clean"),
        F.substring(F.trim(F.col("move_type")), 1, 2).alias("move_type_prefix_2"),
    )
)

metrics = (
    tagged_2
    .select(
        "id",
        "operator",
        "unit_price",
        "operator_clean",
        "unit_price_minus_qty",
        "unit_price_bucket",
        "operator_clean_2",
        "unit_price_minus_qty_adj",
        "move_type_prefix",
        "move_type_clean",
        "move_type_prefix_2",
        F.round(F.cume_dist().over(w_operator_warehouse_id_by_uni), 3).alias("unit_price_minus_qty_adj_cume_dist"),
        F.round(F.avg("unit_price_minus_qty_adj").over(w_operator_unit_price_minus_qt), 2).alias("operator_unit_price_minus_qty_tier_avg_unit_pric"),
        F.rank().over(w_operator_warehouse_id_by_uni_2).alias("unit_price_minus_qty_adj_rank"),
    )
)

annotated = (
    metrics
    .filter(F.length(F.trim(F.col("move_type_clean"))) >= 6)
    .select(
        "*",
        F.round(F.col("unit_price_minus_qty_adj") * 0.25, 2).alias("unit_price_minus_qty_adj_adj"),
    )
)

result = (
    annotated
    .select(
        "id",
        F.col("unit_price_bucket").alias("unit_price_bucket_v"),
        F.col("unit_price_minus_qty_adj_rank").alias("m_unit_price_minus_qty_adj_rank"),
    )
    .unpivot(
        ids=["id"],
        values=["unit_price_bucket_v", "m_unit_price_minus_qty_adj_rank"],
        variableColumnName="field",
        valueColumnName="reading",
    )
    .filter(F.col("reading").isNotNull())
    .orderBy("id", "field")
)
