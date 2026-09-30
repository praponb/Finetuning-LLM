from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id").orderBy("revenue_filled", "id")
w_recent = Window.partitionBy(F.month("sale_date"), "store_id").orderBy("sale_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("store_id").orderBy("sale_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_grp = Window.partitionBy("store_id").orderBy("last_known_revenue_filled", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_part = Window.partitionBy("store_id")
w6 = Window.partitionBy("store_id").orderBy(F.col("seq_no").desc_nulls_last())
w7 = Window.partitionBy("store_id").orderBy("revenue_filled")
w_ordered = Window.partitionBy("store_id").orderBy("running_revenue_filled", "id")
w9 = Window.partitionBy("store_id").orderBy(F.col("seq_no").desc_nulls_last(), "id")
w10 = Window.partitionBy("city")
w11 = Window.partitionBy("store_id").orderBy(F.col("revenue_filled").desc(), "id")

base = (
    df_store_sales
    .filter((F.col("sale_date") >= F.to_date(F.lit("2023-10-01"))) & F.col("store_id").isin([3, 5, 6]))
    .select(
        "id",
        "store_id",
        "sale_date",
        F.coalesce("revenue", F.lit(0)).alias("revenue_filled"),
    )
)

with_windows = (
    base
    .filter(F.col("revenue_filled") >= 27)
    .select(
        "id",
        "store_id",
        "revenue_filled",
        F.row_number().over(w).alias("seq_no"),
        F.sum("revenue_filled").over(w_recent).alias("running_revenue_filled"),
        F.last("revenue_filled", ignorenulls=True).over(w3).alias("last_known_revenue_filled"),
    )
)

enriched = (
    with_windows
    .select(
        "*",
        F.last("revenue_filled", ignorenulls=True).over(w_grp).alias("last_known_revenue_filled_2"),
        F.stddev_samp("last_known_revenue_filled").over(w_part).alias("store_id_stddev_samp_last_known_revenue_filled"),
        F.rank().over(w6).alias("seq_no_rank"),
        F.rank().over(w7).alias("revenue_filled_rank"),
        F.lag("last_known_revenue_filled", 1).over(w_ordered).alias("prev_last_known_revenue_filled"),
    )
)

latest = (
    enriched
    .withColumn("rank_in_group", F.row_number().over(w9))
    .filter(F.col("rank_in_group") == 1)
    .drop("rank_in_group")
)

with_dims = (
    latest
    .alias("s")
    .join(df_stores.alias("p"), F.col("s.store_id") == F.col("p.id"), "inner")
    .select("s.*", F.col("p.city"), F.col("p.opened_date"))
)

enriched_store_sales = (
    with_dims
    .withColumn("city_stddev_samp_prev_last_known_revenue_filled", F.stddev_samp("prev_last_known_revenue_filled").over(w10))
    .withColumn("seq_no_share", F.round((F.col("seq_no") / F.sum("seq_no").over(w10)), 4))
    .withColumn("revenue_filled_bucket", F.ntile(3).over(w11))
)

totals = (
    enriched_store_sales
    .rollup("city", "store_id")
    .agg(
        F.sum(F.when(F.col("city") == "Berlin", F.col("last_known_revenue_filled")).otherwise(0)).alias("berlin_last_known_revenue_filled"),
        F.size(F.collect_set("city")).alias("num_cities"),
        F.sum(F.when(F.col("city") == "Berlin", F.col("last_known_revenue_filled_2")).otherwise(0)).alias("berlin_last_known_revenue_filled_2"),
        F.grouping_id().alias("gid"),
    )
)

result = (
    totals
    .select("city", "berlin_last_known_revenue_filled_2")
    .orderBy(
        F.col("berlin_last_known_revenue_filled_2").asc_nulls_last(),
        F.col("city").desc(),
    )
)
