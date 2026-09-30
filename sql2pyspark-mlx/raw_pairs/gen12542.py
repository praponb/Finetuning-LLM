from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id", "product_id").orderBy(F.col("last_restock_period").asc_nulls_first())
w2 = Window.partitionBy("product_id").orderBy(F.col("last_restock").desc_nulls_first())
w3 = Window.orderBy("adjusted_rank_in_group", "id")
w4 = Window.orderBy("adjusted_rank_in_group", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("store_id").orderBy(F.col("adjusted_rank_in_group_bucket").desc_nulls_first(), "id")

valid = (
    df_inventory
    .filter(F.col("last_restock") >= F.lit("2023-08-06").cast("date"))
    .select(
        "id",
        "store_id",
        "product_id",
        "last_restock",
        F.date_trunc("year", "last_restock").alias("last_restock_period"),
        F.ceil("quantity").alias("quantity_whole"),
    )
)

ranked = (
    valid
    .withColumn("rank_in_group", F.rank().over(w))
    .filter(F.col("rank_in_group") == 1)
)

latest = (
    ranked
    .withColumn("row_num", F.rank().over(w2))
    .filter(F.col("row_num") == 1)
    .select("id", "store_id", "product_id", "last_restock", "rank_in_group")
)

cleaned = (
    latest
    .filter(F.col("last_restock") <= "2025-03-15")
    .select(
        "id",
        "store_id",
        "product_id",
        "last_restock",
        "rank_in_group",
        F.round((F.col("rank_in_group") * 1.15), 2).alias("adjusted_rank_in_group"),
    )
)

filtered = (
    cleaned
    .filter(F.col("store_id").isin([2, 3, 4, 6]))
    .select(
        "*",
        (F.col("rank_in_group") + F.col("adjusted_rank_in_group")).alias("rank_in_group_plus_adjusted_rank_in_group"),
    )
)

with_windows = (
    filtered
    .select(
        "id",
        "store_id",
        "product_id",
        "rank_in_group",
        "adjusted_rank_in_group",
        F.ntile(4).over(w3).alias("adjusted_rank_in_group_bucket"),
        F.sum("rank_in_group_plus_adjusted_rank_in_group").over(w4).alias("cumulative_rank_in_group_plus_adjusted_rank_in_group"),
    )
)

ranked_inventory = (
    with_windows
    .withColumn("latest_rank", F.row_number().over(w5))
    .filter(F.col("latest_rank") == 1)
)

result = (
    ranked_inventory
    .select(
        "id",
        "store_id",
        "cumulative_rank_in_group_plus_adjusted_rank_in_group",
        "latest_rank",
    )
)
