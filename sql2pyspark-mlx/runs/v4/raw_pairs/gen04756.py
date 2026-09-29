from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_part = Window.partitionBy("store_id").orderBy(F.col("quantity_whole").desc_nulls_first())
w_recent = Window.orderBy(F.col("quantity_whole").desc(), "id")
w3 = Window.partitionBy("store_id").orderBy(F.col("quantity").asc_nulls_last())
w4 = Window.partitionBy("store_id").orderBy("quantity_whole_bucket_times_quantity_rank", "id")
w5 = Window.orderBy("quantity_whole_bucket_times_quantity_rank", "id")
w_hist = Window.partitionBy("product_id", "store_id")
w7 = Window.partitionBy("store_id").orderBy("quantity_whole_bucket", "id")
w8 = Window.partitionBy("store_id").orderBy("quantity_rank", "id")
w9 = Window.partitionBy("store_id").orderBy("prev_quantity_whole_plus_quantity_whole_percent_rank", "id")
w10 = Window.partitionBy("store_id", "product_id").orderBy("quantity_whole_bucket", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w11 = Window.partitionBy("product_id").orderBy(F.col("quantity_whole_bucket_times_quantity_rank").desc_nulls_last(), "id")
w12 = Window.partitionBy("store_id").orderBy(F.col("next_product_id_avg_quantity").desc_nulls_first())

base = (
    df_inventory
    .filter(F.col("last_restock") <= "2024-12-01")
    .select("*", F.floor("quantity").alias("quantity_whole"))
)

with_windows = (
    base
    .select(
        "*",
        F.percent_rank().over(w_part).alias("quantity_whole_percent_rank"),
        F.ntile(10).over(w_recent).alias("quantity_whole_bucket"),
        F.rank().over(w3).alias("quantity_rank"),
    )
)

valid = (
    with_windows
    .filter(F.col("quantity_rank").between(4, 7))
    .select(
        "id",
        "store_id",
        "product_id",
        "quantity",
        "quantity_whole",
        "quantity_whole_percent_rank",
        "quantity_whole_bucket",
        "quantity_rank",
        (F.col("quantity_whole_bucket") * F.col("quantity_rank")).alias("quantity_whole_bucket_times_quantity_rank"),
        (F.col("quantity_whole") + F.col("quantity_whole_percent_rank")).alias("quantity_whole_plus_quantity_whole_percent_rank"),
    )
)

scored = (
    valid
    .withColumn("next_quantity_whole", F.lead("quantity_whole", 1).over(w4))
    .withColumn("prev_quantity_whole_plus_quantity_whole_percent_rank", F.lag("quantity_whole_plus_quantity_whole_percent_rank", 1, 0).over(w5))
    .withColumn("product_id_avg_quantity", F.avg("quantity").over(w_hist))
)

scored_inventory = (
    scored
    .select(
        "*",
        F.ntile(5).over(w7).alias("quantity_whole_bucket_bucket"),
        F.lead("product_id_avg_quantity", 1).over(w8).alias("next_product_id_avg_quantity"),
        F.ntile(5).over(w9).alias("prev_quantity_whole_plus_quantity_whole_percent_rank_bucket"),
        F.first("product_id_avg_quantity", ignorenulls=True).over(w10).alias("first_known_product_id_avg_quantity"),
    )
)

deduped = (
    scored_inventory
    .withColumn("rank_in_group", F.row_number().over(w11))
    .filter(F.col("rank_in_group") == 1)
)

deduped_inventory = (
    deduped
    .withColumn("rank_in_group_2", F.rank().over(w12))
    .filter(F.col("rank_in_group_2") == 1)
)

totals = (
    deduped_inventory
    .withColumn("quantity_whole_percent_rank_tier", F.when(F.col("quantity_whole_percent_rank") >= 0.7, "PREMIUM").otherwise("STANDARD"))
    .cube("quantity_whole_percent_rank_tier", "product_id")
    .agg(
        F.bit_and("quantity").alias("quantity_bit_and"),
        F.countDistinct("id").alias("distinct_ids"),
        F.percentile_approx("quantity_whole_bucket", 0.9).alias("quantity_whole_bucket_p90"),
        F.max("quantity_rank").alias("max_quantity_rank"),
        F.bit_or("quantity").alias("quantity_bit_or"),
        F.grouping_id().alias("grouping_id"),
    )
)

result = (
    totals
    .select(
        "quantity_whole_percent_rank_tier",
        "quantity_bit_and",
        "distinct_ids",
        "quantity_whole_bucket_p90",
        "quantity_bit_or",
        "grouping_id",
    )
    .orderBy(F.col("quantity_whole_bucket_p90").asc_nulls_first())
)
