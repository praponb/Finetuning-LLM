from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_recent = Window.partitionBy("store_id", "product_id").orderBy("quantity", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("store_id").orderBy("quantity", "id").rowsBetween(-3, Window.currentRow)
w_grp = Window.orderBy(F.col("quantity_avg_3").desc_nulls_first())
w4 = Window.orderBy("quantity_avg_3")
w5 = Window.orderBy(F.col("running_sum_quantity").desc_nulls_last())
w6 = Window.orderBy("running_count_quantity")
w7 = Window.orderBy(F.col("running_count_quantity_percent_rank").desc_nulls_first())
w8 = Window.orderBy(F.col("running_count_quantity").desc_nulls_last())
w9 = Window.orderBy(F.col("running_count_quantity").asc_nulls_first())
w10 = Window.orderBy(F.col("running_max_quantity").desc_nulls_first())

enriched = (
    df_inventory
    .select(
        "id",
        "quantity",
        F.count("quantity").over(w_recent).alias("running_count_quantity"),
        F.max("quantity").over(w_recent).alias("running_max_quantity"),
        F.sum("quantity").over(w_recent).alias("running_sum_quantity"),
        F.avg("quantity").over(w2).alias("quantity_avg_3"),
    )
)

base = (
    enriched
    .filter(F.col("quantity_avg_3").between(166, 204))
    .select(
        "*",
        (F.col("quantity_avg_3") / F.nullif("running_sum_quantity", F.lit(0))).alias("quantity_avg_3_per_running_sum_quantity"),
    )
)

kept_keys = (
    base
    .select("id")
    .intersectAll(
        df_inventory
        .filter(F.col("store_id").isin([3, 6]))
        .select("id")
    )
)

kept_rows = (
    base
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

cleaned = (
    kept_rows
    .filter(F.col("running_sum_quantity") > 720)
    .withColumn("quantity_avg_3_per_running_sum_quantity_int", F.col("quantity_avg_3_per_running_sum_quantity").cast("int"))
)

enriched_inventory = (
    cleaned
    .withColumn("quantity_avg_3_dense_rank", F.dense_rank().over(w_grp))
    .withColumn("quantity_avg_3_percent_rank", F.percent_rank().over(w4))
    .withColumn("running_sum_quantity_percent_rank", F.percent_rank().over(w5))
    .withColumn("running_count_quantity_percent_rank", F.percent_rank().over(w6))
)

result = (
    enriched_inventory
    .select(
        "running_count_quantity",
        "running_max_quantity",
        "quantity_avg_3_per_running_sum_quantity_int",
        "quantity_avg_3_dense_rank",
        "running_sum_quantity_percent_rank",
        "running_count_quantity_percent_rank",
        F.percent_rank().over(w7).alias("running_count_quantity_percent_rank_percent_rank"),
        F.percent_rank().over(w8).alias("running_count_quantity_percent_rank_2"),
        F.cume_dist().over(w9).alias("running_count_quantity_cume_dist"),
        F.cume_dist().over(w10).alias("running_max_quantity_cume_dist"),
    )
)
