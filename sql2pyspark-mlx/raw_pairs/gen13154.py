from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("region").orderBy("sqft", "id")
w2 = Window.orderBy("sqft", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("city")
w4 = Window.orderBy("prev_sqft")
w5 = Window.orderBy(F.col("sqft").desc(), "id")
w6 = Window.partitionBy("region", "city").orderBy("seq_no", "id")
w_grp = Window.partitionBy("city", "region").orderBy(F.col("prev_sqft").desc_nulls_last())
w8 = Window.partitionBy("region").orderBy(F.col("seq_no_2").desc())
w9 = Window.orderBy(F.col("seq_no").desc(), "id")
w10 = Window.orderBy("latest_rank", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w11 = Window.partitionBy("region").orderBy("latest_rank", "id")

enriched = (
    df_stores
    .select(
        "*",
        F.lag("sqft").over(w).alias("prev_sqft"),
        F.row_number().over(w).alias("seq_no"),
        F.last("region", ignorenulls=True).over(w2).alias("last_known_region"),
    )
)

not_flagged = (
    enriched
    .alias("s")
    .join(df_orders.alias("c"), (F.col("c.store_id") == F.col("s.id")) & (F.col("c.amount") >= 1210), "left_anti")
)

enriched_stores = (
    not_flagged
    .filter(~F.col("city").isin(["Tokyo", "London"]))
    .select(
        "id",
        "store_name",
        "city",
        "region",
        "seq_no",
        "last_known_region",
        F.round((F.col("seq_no") / F.sum("seq_no").over(w3)), 4).alias("seq_no_share"),
        F.cume_dist().over(w4).alias("prev_sqft_cume_dist"),
        F.ntile(3).over(w5).alias("sqft_bucket"),
        F.row_number().over(w6).alias("seq_no_2"),
        F.dense_rank().over(w_grp).alias("prev_sqft_dense_rank"),
    )
)

base = (
    enriched_stores
    .filter(F.col("region").isNotNull())
    .select(
        "id",
        "store_name",
        "city",
        "region",
        "seq_no",
        "last_known_region",
        "prev_sqft_cume_dist",
        "seq_no_2",
        F.ceil("seq_no_2").alias("seq_no_2_whole"),
        F.coalesce("region", F.lit("NONE")).alias("region_filled"),
    )
)

latest = (
    base
    .withColumn("latest_rank", F.rank().over(w8))
    .filter(F.col("latest_rank") == 1)
)

with_windows = (
    latest
    .select(
        "id",
        "city",
        "seq_no",
        "seq_no_2",
        F.row_number().over(w9).alias("seq_no_3"),
        F.sum("seq_no_2").over(w10).alias("cumulative_seq_no_2"),
        F.lag("seq_no_2", 1).over(w11).alias("prev_seq_no_2"),
    )
)

eligible = (
    with_windows
    .alias("s")
    .join(df_inventory.alias("x"), (F.col("x.store_id") == F.col("s.id")) & (F.col("x.last_restock") > "2024-01-01"), "left_semi")
)

result = (
    eligible
    .select(
        "id",
        "city",
        "seq_no",
        "seq_no_2",
        "seq_no_3",
        "cumulative_seq_no_2",
        "prev_seq_no_2",
    )
    .orderBy(F.col("city").desc())
)
