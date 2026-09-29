from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_prior_unit_price_1d_f = Window.orderBy("prior_unit_price_1d", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_prior_unit_price_1d = Window.orderBy("prior_unit_price_1d", "id")
w_sku_id_moved_at_month = Window.partitionBy("sku_id", F.date_trunc("month", "moved_at"))

covered = (
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
        F.sum("b.unit_price").alias("prior_unit_price_1d"),
    )
)

with_details = (
    covered.alias("s")
    .join(df_skus.alias("sk"), F.col("sk.id") == F.col("s.sku_id"), "inner")
    .select(
        "s.id",
        "s.sku_id",
        "s.moved_at",
        "s.prior_unit_price_1d",
        F.col("sk.labels").alias("sku_labels"),
    )
)

sequenced = (
    with_details
    .select(
        "id",
        "sku_id",
        "moved_at",
        "prior_unit_price_1d",
        "sku_labels",
        F.first("prior_unit_price_1d").over(w_all_by_prior_unit_price_1d_f).alias("first_prior_unit_price_1d"),
        F.row_number().over(w_all_by_prior_unit_price_1d).alias("seq_no"),
        F.min("prior_unit_price_1d").over(w_sku_id_moved_at_month).alias("sku_id_moved_at_month_min_prior_unit_price_1d"),
        F.sum("prior_unit_price_1d").over(w_sku_id_moved_at_month).alias("sku_id_moved_at_month_sum_prior_unit_price_1d"),
        F.round((F.unix_timestamp("moved_at") - F.unix_timestamp(F.lag("moved_at").over(w_all_by_prior_unit_price_1d))) / 60, 1).alias("minutes_since_prev_moved_at"),
    )
)

combined_details_sku = (
    sequenced.alias("s")
    .join(df_skus.alias("sk"), (F.col("sk.id") == F.col("s.sku_id")) & (F.col("sk.unit_cost") > 108.5), "inner")
    .select(
        "s.*",
        F.col("sk.unit_cost").alias("sku_unit_cost"),
        F.col("sk.labels").alias("sku_labels_2"),
        "sk.sku_code",
    )
)

result = (
    combined_details_sku
    .rollup("seq_no", "sku_id")
    .agg(F.max("moved_at").alias("last_moved_at"))
    .orderBy(F.col("seq_no").asc_nulls_last(), F.col("sku_id").asc_nulls_last())
)
