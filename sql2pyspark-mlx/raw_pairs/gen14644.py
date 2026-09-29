from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy("quantity", "id")
w2 = Window.partitionBy("product_id")
w3 = Window.partitionBy("store_id")
w4 = Window.partitionBy("store_id").orderBy(F.col("last_restock_dayofweek").desc(), "id")
w5 = Window.partitionBy("product_id").orderBy("last_restock_dayofweek", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_ordered = Window.partitionBy("product_id", "store_id").orderBy(F.col("event_seq_2").desc_nulls_last())
w7 = Window.partitionBy("store_id", "product_id")
w_grp = Window.orderBy("event_seq", "id").rowsBetween(-3, Window.currentRow)

valid = (
    df_inventory
    .filter((F.col("last_restock") > F.lit("2023-07-06").cast("date")) & (F.col("last_restock") < "2024-09-30"))
    .withColumn("last_restock_dayofweek", F.dayofweek("last_restock"))
    .withColumn("quantity_band", F.when(F.col("quantity") >= 261, "High").when(F.col("quantity") >= 110, "Medium").otherwise("Low"))
)

with_windows = (
    valid
    .select(
        "id",
        "store_id",
        "product_id",
        "quantity_band",
        F.row_number().over(w).alias("event_seq"),
        F.stddev_samp("last_restock_dayofweek").over(w2).alias("product_id_stddev_samp_last_restock_dayofweek"),
        F.count("last_restock_dayofweek").over(w3).alias("store_id_count_last_restock_dayofweek"),
        F.row_number().over(w4).alias("event_seq_2"),
        F.sum("last_restock_dayofweek").over(w5).alias("running_last_restock_dayofweek"),
    )
)

scored = (
    with_windows
    .filter(F.col("product_id").isin([1, 5]))
    .select(
        "*",
        F.dense_rank().over(w_ordered).alias("event_seq_2_dense_rank"),
        F.round((F.col("store_id_count_last_restock_dayofweek") / F.sum("store_id_count_last_restock_dayofweek").over(w7)), 4).alias("store_id_count_last_restock_dayofweek_share"),
    )
)

cleaned = (
    scored
    .filter(F.col("event_seq") > 4)
    .select(
        "id",
        "store_id",
        "product_id",
        "quantity_band",
        "event_seq",
        "product_id_stddev_samp_last_restock_dayofweek",
        "store_id_count_last_restock_dayofweek",
        "running_last_restock_dayofweek",
        "event_seq_2_dense_rank",
        "store_id_count_last_restock_dayofweek_share",
        F.round((F.col("product_id_stddev_samp_last_restock_dayofweek") * 1.1), 2).alias("product_id_stddev_samp_last_restock_dayofweek_scaled"),
    )
)

joined = (
    cleaned
    .alias("a")
    .join(df_stores.alias("p"), F.col("a.store_id") == F.col("p.id"), "left")
    .select("a.*", F.col("p.store_name"))
)

filtered = (
    joined
    .filter(F.col("store_name").isNotNull())
    .select(
        "id",
        "quantity_band",
        "event_seq",
        "product_id_stddev_samp_last_restock_dayofweek",
        "store_id_count_last_restock_dayofweek",
        "running_last_restock_dayofweek",
        "store_id_count_last_restock_dayofweek_share",
        "product_id_stddev_samp_last_restock_dayofweek_scaled",
        "store_name",
        F.regexp_replace("store_name", " ", "_").alias("store_name_slug"),
    )
)

scored_inventory = (
    filtered
    .withColumn("last4_count_event_seq", F.count("event_seq").over(w_grp))
    .withColumn("last4_min_event_seq", F.min("event_seq").over(w_grp))
    .withColumn("last4_max_event_seq", F.max("event_seq").over(w_grp))
)

result = (
    scored_inventory
    .select(
        "id",
        "quantity_band",
        "event_seq",
        "product_id_stddev_samp_last_restock_dayofweek",
        "store_id_count_last_restock_dayofweek",
        "store_name",
        "store_name_slug",
        "last4_min_event_seq",
    )
    .orderBy(F.col("event_seq").asc_nulls_first())
)
