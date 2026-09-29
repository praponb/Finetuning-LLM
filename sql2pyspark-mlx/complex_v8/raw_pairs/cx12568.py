from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_prior_rows_1d_full = Window.orderBy(F.col("prior_rows_1d").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_moved_at_month_by_prior_qty = Window.partitionBy(F.month("moved_at")).orderBy("prior_qty_1d", "id").rowsBetween(-2, -1)
w_sku_id_moved_at_year_by_prio = Window.partitionBy("sku_id", F.year("moved_at")).orderBy(F.col("prior_qty_1d").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_moved_at_month_by_prior_qty_2 = Window.partitionBy(F.month("moved_at")).orderBy("prior_qty_1d", "id")
w_moved_at_year = Window.partitionBy(F.year("moved_at"))

joined_ranges = (
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

with_trends = (
    joined_ranges
    .select(
        "*",
        F.last("prior_rows_1d").over(w_all_by_prior_rows_1d_full).alias("last_prior_rows_1d"),
        F.round(F.avg("prior_qty_1d").over(w_moved_at_month_by_prior_qty), 2).alias("prior_qty_1d_avg_2_prior"),
        F.first("prior_rows_1d").over(w_sku_id_moved_at_year_by_prio).alias("first_prior_rows_1d"),
        F.lead("moved_at").over(w_moved_at_month_by_prior_qty_2).alias("next_moved_at"),
        F.size(F.collect_set("sku_id").over(w_moved_at_year)).alias("distinct_sku_id_in_moved_at_year"),
        F.last("moved_at").over(w_sku_id_moved_at_year_by_prio).alias("last_moved_at"),
    )
)

result = (
    with_trends
    .select(
        "id",
        F.col("distinct_sku_id_in_moved_at_year").alias("distinct_sku_id_in_moved_at_year_v"),
        F.col("prior_qty_1d").alias("prior_qty_1d_v"),
    )
    .unpivot(
        ids=["id"],
        values=["distinct_sku_id_in_moved_at_year_v", "prior_qty_1d_v"],
        variableColumnName="field",
        valueColumnName="value",
    )
    .filter(F.col("value").isNotNull())
    .orderBy("id", "field")
)
