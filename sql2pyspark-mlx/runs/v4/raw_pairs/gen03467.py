from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_grp = Window.orderBy(F.col("qty").desc_nulls_last())
w2 = Window.orderBy("price")
w3 = Window.orderBy(F.col("price_cume_dist").desc())
w4 = Window.orderBy(F.col("qty_cume_dist").asc_nulls_first())
w5 = Window.orderBy(F.col("price").asc_nulls_last())

lines = (
    df_carts
    .withColumn("li_item", F.explode("items"))
    .select("id", F.col("li_item.qty"), F.col("li_item.price"))
)

cleaned = (
    lines
    .filter(F.col("price") >= 150)
    .withColumn("price_plus_qty", (F.col("price") + F.col("qty")))
)

enriched = (
    cleaned
    .select(
        "*",
        F.cume_dist().over(w_grp).alias("qty_cume_dist"),
        F.cume_dist().over(w2).alias("price_cume_dist"),
    )
)

kept_keys = (
    enriched
    .select("id")
    .exceptAll(
        df_carts
        .filter(F.to_date("created_at") >= "2024-04-17")
        .select("id")
    )
)

reconciled = (
    enriched
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

with_windows = (
    reconciled
    .withColumn("price_cume_dist_rank", F.rank().over(w3))
    .withColumn("qty_cume_dist_rank", F.rank().over(w4))
    .withColumn("price_percent_rank", F.percent_rank().over(w5))
)

summary = (
    with_windows
    .groupBy(
        F.when(F.col("price_cume_dist") >= 0.71, "LARGE").otherwise("SMALL").alias("price_cume_dist_tier"),
    )
    .agg(
        F.count("*").alias("num_rows"),
        F.count("*").alias("cnt"),
        F.count("*").alias("row_count"),
    )
)

result = summary.select("price_cume_dist_tier", "num_rows", "cnt", "row_count")
