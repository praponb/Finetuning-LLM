from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id", "product_id")
w2 = Window.partitionBy("store_id").orderBy(F.col("units").desc_nulls_last())
w3 = Window.partitionBy("store_id")

kept_keys = (
    df_store_sales
    .select("id", "product_id")
    .subtract(
        df_store_sales
        .filter(F.col("product_id") == 4)
        .select("id", "product_id")
    )
)

reconciled = (
    df_store_sales
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.product_id") == F.col("k.product_id")), "inner")
    .select("s.*")
)

filtered = (
    reconciled
    .filter(F.col("sale_date") >= F.lit("2023-12-20").cast("date"))
    .select(
        "store_id",
        "product_id",
        "sale_date",
        "units",
        F.dayofmonth("sale_date").alias("sale_date_day"),
        F.date_trunc("WEEK", "sale_date").alias("sale_date_period"),
    )
)

enriched = (
    filtered
    .select(
        "*",
        F.stddev_samp("sale_date_day").over(w).alias("store_id_stddev_samp_sale_date_day"),
        F.dense_rank().over(w2).alias("units_dense_rank"),
        F.count("units").over(w3).alias("store_id_count_units"),
    )
)

valid = (
    enriched
    .filter(F.col("store_id") == 2)
    .select(
        "*",
        (F.col("units_dense_rank") * F.col("sale_date_day")).alias("units_dense_rank_times_sale_date_day"),
        F.round((F.col("store_id_stddev_samp_sale_date_day") / F.col("store_id_count_units")), 2).alias("store_id_stddev_samp_sale_date_day_per_store_id_count_units"),
    )
)

result = (
    valid
    .rollup("store_id")
    .agg(
        F.count("*").alias("num_rows"),
        F.bit_or("units").alias("units_bit_or"),
    )
    .filter(F.col("units_bit_or") < 1)
    .orderBy(F.col("units_bit_or").asc_nulls_last())
)
