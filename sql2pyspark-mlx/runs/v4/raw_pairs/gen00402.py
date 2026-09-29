from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_grp = Window.partitionBy("product_id").orderBy("price_whole")
w_hist = Window.orderBy(F.col("line_total").desc())
w_grp_2 = Window.orderBy(F.col("price_whole").desc_nulls_last())
w_part = Window.orderBy(F.col("price_whole").asc_nulls_first())
w5 = Window.partitionBy("order_id", "product_id")
w6 = Window.orderBy(F.col("price_whole_percent_rank").desc())
w7 = Window.partitionBy("order_id").orderBy(F.col("line_total").desc())

remaining_keys = (
    df_order_items
    .select("product_id")
    .exceptAll(
        df_order_items
        .filter(F.col("product_id") == 4)
        .select("product_id")
    )
)

kept_rows = (
    df_order_items
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.product_id") == F.col("k.product_id")), "inner")
    .select("s.*")
)

cleaned = (
    kept_rows
    .filter(F.col("product_id").isNotNull())
    .withColumn("line_total", (F.col("quantity") + F.col("unit_price")))
    .withColumn("unit_price_level", F.when(F.col("unit_price") >= 390, "High").when(F.col("unit_price") >= 130, "Medium").otherwise("Low"))
)

enriched = (
    cleaned
    .alias("s")
    .join(df_products.alias("b"), F.col("s.product_id") == F.col("b.id"), "left")
    .select("s.*", F.col("b.price"), F.col("b.category"), F.col("b.brand"))
)

filtered = (
    enriched
    .filter(F.col("product_id").isNotNull())
    .select(
        "id",
        "order_id",
        "product_id",
        "unit_price",
        "line_total",
        "unit_price_level",
        "brand",
        (F.col("quantity") + F.col("unit_price")).alias("line_total_2"),
        F.ceil("price").alias("price_whole"),
        F.coalesce("category", F.lit("OTHER")).alias("category_clean"),
    )
)

enriched_order_items = (
    filtered
    .withColumn("price_whole_percent_rank", F.percent_rank().over(w_grp))
    .withColumn("line_total_percent_rank", F.percent_rank().over(w_hist))
    .withColumn("price_whole_percent_rank_2", F.percent_rank().over(w_grp_2))
)

enriched_order_items_2 = (
    enriched_order_items
    .select(
        "*",
        F.percent_rank().over(w_part).alias("price_whole_percent_rank_3"),
        F.avg("line_total_percent_rank").over(w5).alias("order_id_avg_line_total_percent_rank"),
        F.cume_dist().over(w6).alias("price_whole_percent_rank_cume_dist"),
        F.percent_rank().over(w7).alias("line_total_percent_rank_2"),
    )
)

result = (
    enriched_order_items_2
    .select(
        "id",
        "order_id",
        "product_id",
        "line_total",
        "unit_price_level",
        "price_whole",
        "category_clean",
        "price_whole_percent_rank",
        "line_total_percent_rank",
    )
    .orderBy("line_total", "price_whole_percent_rank")
)
