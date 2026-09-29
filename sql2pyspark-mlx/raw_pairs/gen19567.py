from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city").orderBy(F.col("opened_date").desc_nulls_last(), "id")
w2 = Window.orderBy(F.col("sqft").desc())
w3 = Window.partitionBy("city").orderBy("sqft", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("city").orderBy("opened_date", "id")
w5 = Window.partitionBy("city", "region")
w6 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.last_restock").desc(), F.col("c.id").desc())
w7 = Window.partitionBy("city").orderBy("last_restock", "id").rowsBetween(-5, -1)
w8 = Window.partitionBy("city").orderBy("sqft_share", "id").rowsBetween(-3, -1)
w9 = Window.orderBy(F.col("inventory_quantity").asc_nulls_last())
w10 = Window.partitionBy("city").orderBy("sqft_share", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w11 = Window.partitionBy("city").orderBy("inventory_quantity", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_recent = Window.partitionBy("city").orderBy("inventory_quantity_rank", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

ranked = (
    df_stores
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") == 1)
)

eligible = (
    ranked
    .alias("s")
    .join(df_inventory.alias("ch"), (F.col("ch.store_id") == F.col("s.id")) & (F.col("ch.last_restock") <= F.col("s.opened_date")), "left_anti")
)

metrics = (
    eligible
    .select(
        "id",
        "store_name",
        "city",
        "sqft",
        F.percent_rank().over(w2).alias("sqft_percent_rank"),
        F.first("region", ignorenulls=True).over(w3).alias("first_known_region"),
        F.lead("sqft", 2, 0).over(w4).alias("next_sqft"),
        F.round((F.col("sqft") / F.sum("sqft").over(w5)), 4).alias("sqft_share"),
    )
)

with_latest = (
    metrics
    .alias("s")
    .join(df_inventory.alias("c"), (F.col("c.store_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w6))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.store_name",
        "s.city",
        "s.sqft",
        "s.sqft_share",
        F.col("c.quantity").alias("inventory_quantity"),
        F.col("c.last_restock"),
    )
)

with_windows = (
    with_latest
    .select(
        "id",
        "store_name",
        "city",
        "sqft",
        "sqft_share",
        F.avg("sqft").over(w7).alias("sqft_avg_5_prior"),
        F.avg("sqft_share").over(w8).alias("sqft_share_avg_3_prior"),
        F.rank().over(w9).alias("inventory_quantity_rank"),
        F.sum("sqft_share").over(w10).alias("running_sqft_share"),
        F.sum("sqft").over(w11).alias("cumulative_sqft"),
    )
)

with_windows_stores = (
    with_windows
    .select(
        "id",
        "store_name",
        "sqft_share",
        "running_sqft_share",
        F.sum("sqft_avg_5_prior").over(w_recent).alias("running_sum_sqft_avg_5_prior"),
        F.avg("sqft_avg_5_prior").over(w_recent).alias("running_avg_sqft_avg_5_prior"),
        F.count("sqft_avg_5_prior").over(w_recent).alias("running_count_sqft_avg_5_prior"),
    )
)

eligible_stores = (
    with_windows_stores
    .alias("s")
    .join(df_inventory.alias("ch"), (F.col("ch.store_id") == F.col("s.id")) & (F.col("ch.store_id") == 1), "left_semi")
)

result = (
    eligible_stores
    .select(
        "id",
        "running_sum_sqft_avg_5_prior",
        "running_avg_sqft_avg_5_prior",
        "sqft_share",
    )
    .unpivot(
        ids=["id"],
        values=["running_sum_sqft_avg_5_prior", "running_avg_sqft_avg_5_prior", "sqft_share"],
        variableColumnName="metric_name",
        valueColumnName="val",
    )
    .filter(F.col("val").isNotNull())
)
