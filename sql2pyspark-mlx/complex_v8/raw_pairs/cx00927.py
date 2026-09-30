from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_latest = Window.partitionBy("sku_id").orderBy("prior_unit_price_1d", "id")

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

latest = (
    covered
    .withColumn("row_num", F.row_number().over(w_latest))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

rollup_base = (
    latest
    .groupBy("sku_id")
    .agg(
        F.percentile_approx("prior_unit_price_1d", 0.5).alias("median_prior_unit_price_1d"),
        F.count("*").alias("cnt"),
        F.max("moved_at").alias("last_moved_at"),
        F.min("prior_rows_1d").alias("min_prior_rows_1d"),
        F.countDistinct("id").alias("distinct_ids"),
        F.round(F.stddev("prior_rows_1d"), 2).alias("stddev_prior_rows_1d"),
    )
)

result = (
    rollup_base
    .groupingSets(
        [["sku_id", "cnt", "min_prior_rows_1d"], ["sku_id"], []],
        "sku_id", "cnt", "min_prior_rows_1d",
    )
    .agg(F.max("last_moved_at").alias("last_last_moved_at"))
    .orderBy(
        F.col("sku_id").asc_nulls_last(),
        F.col("cnt").asc_nulls_last(),
        F.col("min_prior_rows_1d").asc_nulls_last(),
    )
)
