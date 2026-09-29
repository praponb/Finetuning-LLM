from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_sku_id_prior_rows_12h_by_pri = Window.partitionBy("sku_id", "prior_rows_12h").orderBy("prior_rows_12h", "id")

joined_ranges = (
    df_stock_moves.alias("a")
    .join(
        df_stock_moves.alias("b"),
        (F.col("b.sku_id") == F.col("a.sku_id"))
        & (F.col("b.moved_at") < F.col("a.moved_at"))
        & (F.col("b.moved_at") >= F.col("a.moved_at") - F.expr("INTERVAL 12 HOURS")),
        "left",
    )
    .groupBy("a.id", "a.sku_id", "a.moved_at")
    .agg(
        F.count("b.id").alias("prior_rows_12h"),
        F.sum("b.qty").alias("prior_qty_12h"),
    )
)

annotated = joined_ranges.select("*", F.date_trunc("year", F.col("moved_at")).alias("moved_at_year"))

sequenced = (
    annotated
    .select(
        "*",
        F.row_number().over(w_sku_id_prior_rows_12h_by_pri).alias("seq_no"),
        (F.col("prior_rows_12h") - F.lag("prior_rows_12h").over(w_sku_id_prior_rows_12h_by_pri)).alias("prior_rows_12h_change"),
        F.lead("prior_rows_12h").over(w_sku_id_prior_rows_12h_by_pri).alias("next_prior_rows_12h"),
        F.round((F.unix_timestamp("moved_at_year") - F.unix_timestamp(F.lag("moved_at_year").over(w_sku_id_prior_rows_12h_by_pri))) / 60, 1).alias("minutes_since_prev_moved_at_year"),
        F.lag("prior_qty_12h").over(w_sku_id_prior_rows_12h_by_pri).alias("prev_prior_qty_12h"),
    )
)

result = (
    sequenced
    .select(
        "id",
        "sku_id",
        "moved_at",
        "prior_rows_12h",
        "prior_qty_12h",
        "moved_at_year",
        "seq_no",
        "prior_rows_12h_change",
        "next_prior_rows_12h",
        "prev_prior_qty_12h",
    )
    .orderBy("prior_rows_12h", F.col("id").asc())
)
