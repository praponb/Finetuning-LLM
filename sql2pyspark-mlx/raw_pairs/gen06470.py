from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.discount").desc(), F.col("c.id").desc())
w2 = Window.partitionBy(F.month("opened_date"), "region")
w3 = Window.orderBy("order_amount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_ordered = Window.partitionBy(F.year("opened_date"), "channel").orderBy(F.col("order_amount").asc_nulls_first())
w5 = Window.partitionBy("channel").orderBy("order_amount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.partitionBy("channel").orderBy("opened_date", "id").rowsBetween(-5, -1)
w_grp = Window.partitionBy(F.date_trunc("YEAR", "opened_date"), "region").orderBy(F.col("sqft_share").asc_nulls_first())
w8 = Window.partitionBy("channel").orderBy("opened_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

valid = (
    df_stores
    .filter((F.col("opened_date") >= F.lit("2023-02-27").cast("date")) & F.col("store_name").like("%Flags%"))
    .select(
        "id",
        "store_name",
        "region",
        "opened_date",
        "sqft",
        F.substring("store_name", 1, 2).alias("store_name_prefix"),
    )
)

latest_match = (
    valid
    .alias("s")
    .join(df_orders.alias("c"), (F.col("c.store_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.channel"), F.col("c.amount").alias("order_amount"))
)

enriched = (
    latest_match
    .select(
        "*",
        F.round((F.col("sqft") / F.sum("sqft").over(w2)), 4).alias("sqft_share"),
        F.first("region", ignorenulls=True).over(w3).alias("first_known_region"),
        F.rank().over(w_ordered).alias("order_amount_rank"),
    )
)

with_windows = (
    enriched
    .select(
        "*",
        F.sum("order_amount_rank").over(w5).alias("cumulative_order_amount_rank"),
        F.avg("sqft_share").over(w6).alias("sqft_share_avg_5_prior"),
        F.dense_rank().over(w_grp).alias("sqft_share_dense_rank"),
        F.sum("order_amount_rank").over(w8).alias("cumulative_order_amount_rank_2"),
    )
)

result = (
    with_windows
    .select(
        "id",
        "store_name",
        "sqft",
        "store_name_prefix",
        "order_amount",
        "sqft_share",
        "order_amount_rank",
        "sqft_share_avg_5_prior",
        "sqft_share_dense_rank",
    )
    .orderBy(F.col("sqft_share_dense_rank").desc(), "order_amount_rank")
)
