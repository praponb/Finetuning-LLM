from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy(F.col("sale_date").desc_nulls_last(), "id")
w2 = Window.partitionBy("product_id")
w3 = Window.partitionBy("store_id")
w4 = Window.partitionBy("product_id", "store_id").orderBy(F.col("units").desc_nulls_last())

ranked = (
    df_store_sales
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
    .select("id", "store_id", "product_id", "units")
)

remaining_keys = (
    ranked
    .select("id", "product_id")
    .exceptAll(
        df_store_sales
        .filter(F.col("product_id").isin([2, 4, 5, 9]))
        .select("id", "product_id")
    )
)

verified = (
    ranked
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.product_id") == F.col("k.product_id")), "inner")
    .select("s.*")
)

kept_keys = (
    verified
    .select("product_id", "id")
    .intersectAll(
        df_store_sales
        .filter(F.col("product_id").isin([3, 10]))
        .select("product_id", "id")
    )
)

verified_store_sales = (
    verified
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.product_id") == F.col("k.product_id")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

with_windows = (
    verified_store_sales
    .select(
        "id",
        "store_id",
        F.round((F.col("units") / F.sum("units").over(w2)), 4).alias("units_share"),
        F.sum("units").over(w2).alias("product_id_sum_units"),
        F.round((F.col("units") / F.sum("units").over(w3)), 4).alias("units_share_2"),
        F.avg("units").over(w2).alias("product_id_avg_units"),
        F.cume_dist().over(w4).alias("units_cume_dist"),
    )
)

result = (
    with_windows
    .select("id", "store_id", "units_share", "product_id_avg_units")
    .orderBy("units_share")
)
