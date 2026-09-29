from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("region").orderBy("sqft")
w2 = Window.partitionBy("city").orderBy(F.col("sqft").desc_nulls_first())

eligible = (
    df_stores
    .alias("s")
    .join(df_orders.alias("ch"), (F.col("ch.store_id") == F.col("s.id")) & (F.col("ch.order_date") <= F.col("s.opened_date")), "left_anti")
)

remaining_keys = (
    eligible
    .select("id", "region")
    .subtract(
        df_stores
        .filter(F.year("opened_date") >= 2025)
        .select("id", "region")
    )
)

verified = (
    eligible
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.region") == F.col("k.region")), "inner")
    .select("s.*")
)

metrics = (
    verified
    .filter(F.col("region").isNull())
    .select(
        "*",
        F.cume_dist().over(w).alias("sqft_cume_dist"),
        F.dense_rank().over(w2).alias("sqft_dense_rank"),
    )
)

filtered = (
    metrics
    .filter(F.col("opened_date") >= F.to_date(F.lit("2023-03-13")))
    .select(
        "store_name",
        "city",
        "region",
        "opened_date",
        "sqft",
        "sqft_cume_dist",
        "sqft_dense_rank",
        F.when(F.col("city") == "Berlin", 1).otherwise(0).alias("is_berlin"),
    )
)

result = (
    filtered
    .groupBy("region")
    .pivot("city", ["Berlin", "Bangkok"])
    .agg(F.max("sqft_dense_rank"))
)
