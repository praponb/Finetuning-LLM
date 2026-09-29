from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id").orderBy("quantity", "id")
w2 = Window.orderBy("last_restock", "id")
w3 = Window.partitionBy("product_id", "store_id")
w4 = Window.partitionBy("store_id", "product_id").orderBy("last_restock", "id")
w5 = Window.orderBy(F.col("next_quantity_2").desc())
w6 = Window.orderBy("product_id_stddev_samp_quantity", "id").rowsBetween(-3, Window.currentRow)
w7 = Window.orderBy("product_id_stddev_samp_quantity")
w_hist = Window.partitionBy(F.date_trunc("month", "last_restock"))
w9 = Window.orderBy("quantity_bucket", "id").rowsBetween(-6, -1)
w10 = Window.orderBy("last_restock", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

enriched = (
    df_inventory
    .filter(F.col("quantity") > 231)
    .select(
        "id",
        "last_restock",
        F.ntile(10).over(w).alias("quantity_bucket"),
        F.lead("quantity", 2).over(w2).alias("next_quantity"),
        F.stddev_samp("quantity").over(w3).alias("product_id_stddev_samp_quantity"),
        F.lead("quantity", 1).over(w4).alias("next_quantity_2"),
    )
)

with_windows = (
    enriched
    .select(
        "*",
        F.cume_dist().over(w5).alias("next_quantity_2_cume_dist"),
        F.avg("quantity_bucket").over(w6).alias("quantity_bucket_avg_3"),
        F.cume_dist().over(w7).alias("product_id_stddev_samp_quantity_cume_dist"),
    )
)

metrics = (
    with_windows
    .withColumn("last_restock_period_stddev_samp_quantity_bucket", F.stddev_samp("quantity_bucket").over(w_hist))
    .withColumn("quantity_bucket_avg_6_prior", F.avg("quantity_bucket").over(w9))
    .withColumn("first_known_next_quantity_2_cume_dist", F.first("next_quantity_2_cume_dist", ignorenulls=True).over(w10))
)

matched_keys = (
    metrics
    .select("id")
    .intersect(
        df_inventory
        .filter(F.col("quantity").between(123, 194))
        .select("id")
    )
)

reconciled = (
    metrics
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

result = (
    reconciled
    .select(
        "quantity_bucket",
        "next_quantity_2",
        "last_restock_period_stddev_samp_quantity_bucket",
        "quantity_bucket_avg_6_prior",
        "first_known_next_quantity_2_cume_dist",
    )
    .orderBy(
        F.col("first_known_next_quantity_2_cume_dist").desc_nulls_last(),
        "next_quantity_2",
    )
)
