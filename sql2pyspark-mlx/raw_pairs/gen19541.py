from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy("last_restock_weekofyear", "id")
w2 = Window.partitionBy("product_id", "store_id").orderBy(F.col("quantity").desc())
w3 = Window.partitionBy("product_id").orderBy("last_restock_weekofyear", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("store_id").orderBy(F.col("rank_in_group").desc(), "id")
w5 = Window.partitionBy("store_id").orderBy(F.col("last_restock_weekofyear").desc(), "id")
w6 = Window.orderBy("rank_in_group_2", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w7 = Window.partitionBy("product_id")
w8 = Window.partitionBy("product_id").orderBy(F.col("product_id_min_quantity").desc(), "id")
w9 = Window.partitionBy("last_restock_weekofyear_tier").orderBy(F.col("avg_last_restock_weekofyear_bucket").desc())

valid = (
    df_inventory
    .filter(F.col("store_id").isin([1, 2, 3, 6]))
    .select(
        "id",
        "store_id",
        "product_id",
        "quantity",
        F.weekofyear("last_restock").alias("last_restock_weekofyear"),
    )
)

deduped = (
    valid
    .withColumn("rank_in_group", F.row_number().over(w))
    .filter(F.col("rank_in_group") == 1)
    .select("id", "store_id", "product_id", "quantity", "last_restock_weekofyear")
)

ranked = (
    deduped
    .withColumn("rank_in_group", F.rank().over(w2))
    .filter(F.col("rank_in_group") == 1)
)

latest = (
    ranked
    .withColumn("rank_in_group_2", F.row_number().over(w))
    .filter(F.col("rank_in_group_2") == 1)
)

enriched = (
    latest
    .filter(F.col("rank_in_group_2") <= 1)
    .select(
        "*",
        F.sum("quantity").over(w3).alias("running_quantity"),
        F.row_number().over(w4).alias("row_seq"),
        F.ntile(4).over(w5).alias("last_restock_weekofyear_bucket"),
        F.first("quantity", ignorenulls=True).over(w6).alias("first_known_quantity"),
        F.min("quantity").over(w7).alias("product_id_min_quantity"),
    )
)

deduped_inventory = (
    enriched
    .withColumn("rank_in_group_3", F.row_number().over(w8))
    .filter(F.col("rank_in_group_3") == 1)
)

with_dims = (
    deduped_inventory
    .alias("s")
    .join(df_products.alias("p"), F.col("s.product_id") == F.col("p.id"), "left")
    .select(
        "s.id",
        "s.store_id",
        "s.product_id",
        "s.last_restock_weekofyear",
        "s.rank_in_group",
        "s.last_restock_weekofyear_bucket",
        F.col("p.product_name"),
        F.col("p.launch_date"),
    )
)

daily = (
    with_dims
    .groupBy(
        F.when(F.col("last_restock_weekofyear") >= 84, "HIGH").otherwise("LOW").alias("last_restock_weekofyear_tier"),
        F.date_trunc("WEEK", "launch_date").alias("week_start"),
    )
    .agg(
        F.round(F.avg("last_restock_weekofyear_bucket"), 2).alias("avg_last_restock_weekofyear_bucket"),
        F.percentile_approx("last_restock_weekofyear", 0.5).alias("last_restock_weekofyear_p50"),
        F.countDistinct("id").alias("distinct_ids"),
    )
    .withColumn("avg_last_restock_weekofyear_bucket_dense_rank", F.dense_rank().over(w9))
)

result = (
    daily
    .select(
        "last_restock_weekofyear_tier",
        "week_start",
        "avg_last_restock_weekofyear_bucket",
        "last_restock_weekofyear_p50",
        "distinct_ids",
    )
    .orderBy(F.col("avg_last_restock_weekofyear_bucket").desc_nulls_last())
)
