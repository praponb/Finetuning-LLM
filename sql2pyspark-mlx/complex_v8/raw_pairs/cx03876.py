from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_prior_rows_3d_moved_at_month = Window.partitionBy("prior_rows_3d", F.month("moved_at")).orderBy("prior_unit_price_3d", "id")
w_prior_rows_3d_moved_at_month_2 = Window.partitionBy("prior_rows_3d", F.month("moved_at")).orderBy("prior_unit_price_3d", "id").rowsBetween(-6, Window.currentRow)
w_sku_id = Window.partitionBy("sku_id")
w_moved_at_month_sku_id_by_pri = Window.partitionBy(F.date_trunc("month", "moved_at"), "sku_id").orderBy("prior_rows_3d", "id")
w_latest = Window.partitionBy("sku_id").orderBy(F.col("sku_id_min_prior_unit_price_3d").desc(), F.col("id").desc())

joined_ranges = (
    df_stock_moves.alias("a")
    .join(
        df_stock_moves.alias("b"),
        (F.col("b.sku_id") == F.col("a.sku_id"))
        & (F.col("b.moved_at") < F.col("a.moved_at"))
        & (F.col("b.moved_at") >= F.col("a.moved_at") - F.expr("INTERVAL 3 DAYS")),
        "left",
    )
    .groupBy("a.id", "a.sku_id", "a.moved_at")
    .agg(
        F.count("b.id").alias("prior_rows_3d"),
        F.sum("b.unit_price").alias("prior_unit_price_3d"),
    )
)

with_trends = (
    joined_ranges
    .select(
        "*",
        (F.col("prior_unit_price_3d") - F.lag("prior_unit_price_3d").over(w_prior_rows_3d_moved_at_month)).alias("prior_unit_price_3d_change"),
        F.round(F.avg("prior_unit_price_3d").over(w_prior_rows_3d_moved_at_month_2), 2).alias("prior_unit_price_3d_avg_7"),
        F.min("prior_unit_price_3d").over(w_sku_id).alias("sku_id_min_prior_unit_price_3d"),
        F.row_number().over(w_moved_at_month_sku_id_by_pri).alias("rn"),
        F.lead("moved_at").over(w_prior_rows_3d_moved_at_month).alias("next_moved_at"),
    )
)

deduped = (
    with_trends
    .withColumn("recency_rank", F.row_number().over(w_latest))
    .filter(F.col("recency_rank") == 1)
    .drop("recency_rank")
)

annotated = (
    deduped
    .select(
        "*",
        F.round(F.col("prior_unit_price_3d") * 0.9, 2).alias("prior_unit_price_3d_adj"),
        F.to_date(F.col("moved_at")).alias("moved_at_date"),
    )
)

result = (
    annotated
    .filter(F.col("rn") > 3)
    .select(
        "id",
        "sku_id",
        "moved_at",
        "prior_rows_3d",
        "prior_unit_price_3d",
        "prior_unit_price_3d_change",
        "prior_unit_price_3d_avg_7",
        "sku_id_min_prior_unit_price_3d",
        "rn",
        "next_moved_at",
        "prior_unit_price_3d_adj",
        "moved_at_date",
        (F.col("prior_unit_price_3d_adj") - F.col("prior_rows_3d")).alias("prior_unit_price_3d_adj_minus_prior_rows_3d"),
        F.date_add(F.col("moved_at_date"), 90).alias("moved_at_date_plus_90d"),
    )
    .orderBy(F.col("prior_unit_price_3d_change").desc(), F.col("sku_id").desc())
)
