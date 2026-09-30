from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_prior_rows_1d_by_prior_qty_1 = Window.partitionBy("prior_rows_1d").orderBy("prior_qty_1d", "id").rowsBetween(-6, Window.currentRow)
w_sku_id = Window.partitionBy("sku_id")
w_prior_rows_1d_by_prior_qty_1_2 = Window.partitionBy("prior_rows_1d").orderBy("prior_qty_1d")
w_prior_rows_1d_by_prior_qty_1_3 = Window.partitionBy("prior_rows_1d").orderBy("prior_qty_1d", "id")

in_window = (
    df_stock_moves.alias("a")
    .join(
        df_stock_moves.alias("b"),
        (F.col("b.sku_id") == F.col("a.sku_id"))
        & (F.col("b.moved_at") < F.col("a.moved_at"))
        & (F.col("b.moved_at") >= F.col("a.moved_at") - F.expr("INTERVAL 1 DAYS")),
        "left",
    )
    .groupBy("a.id", "a.sku_id", "a.moved_at")
    .agg(
        F.count("b.id").alias("prior_rows_1d"),
        F.sum("b.qty").alias("prior_qty_1d"),
    )
)

sequenced = (
    in_window
    .select(
        "id",
        "sku_id",
        "moved_at",
        "prior_qty_1d",
        F.max("prior_qty_1d").over(w_prior_rows_1d_by_prior_qty_1).alias("prior_qty_1d_max_7"),
        F.count(F.lit(1)).over(w_sku_id).alias("rows_in_sku_id"),
        F.dense_rank().over(w_prior_rows_1d_by_prior_qty_1_2).alias("prior_qty_1d_dense_rank"),
        F.lag("prior_rows_1d").over(w_prior_rows_1d_by_prior_qty_1_3).alias("prev_prior_rows_1d"),
        F.lag("moved_at").over(w_prior_rows_1d_by_prior_qty_1_3).alias("prev_moved_at"),
    )
)

result = (
    sequenced
    .select(
        "id",
        "moved_at",
        "prior_qty_1d",
        "rows_in_sku_id",
        "prev_prior_rows_1d",
        "prev_moved_at",
        F.round((F.unix_timestamp(F.col("moved_at")) - F.unix_timestamp(F.col("prev_moved_at"))) / 60, 1).alias("minutes_prev_moved_at_to_moved_at"),
        F.year(F.col("prev_moved_at")).alias("prev_moved_at_year"),
    )
    .orderBy(F.col("id").asc())
)
