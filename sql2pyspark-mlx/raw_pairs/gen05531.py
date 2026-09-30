from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("brand")
w2 = Window.partitionBy("category").orderBy("price")
w3 = Window.partitionBy("category").orderBy("stock_qty", "id").rowsBetween(-2, -1)
w4 = Window.partitionBy("country").orderBy("price_avg_2_prior", "id").rowsBetween(-5, Window.currentRow)
w5 = Window.partitionBy("city").orderBy(F.col("stock_qty").desc_nulls_first())
w6 = Window.partitionBy("city").orderBy("stock_qty", "id").rowsBetween(-6, Window.currentRow)

filtered = (
    df_products
    .filter(F.col("brand") == "Acme")
    .select(
        "*",
        F.array_join("tags", ", ").alias("tags_text"),
        F.coalesce("brand", F.lit("UNKNOWN")).alias("resolved_brand"),
    )
)

metrics = (
    filtered
    .withColumn("corr_cost_price", F.corr("cost", "price").over(w))
    .withColumn("price_dense_rank", F.dense_rank().over(w2))
    .withColumn("price_avg_2_prior", F.avg("price").over(w3))
)

kept = (
    metrics
    .alias("s")
    .join(df_inventory.alias("ch"), (F.col("ch.product_id") == F.col("s.id")) & (F.col("ch.last_restock") <= "2023-09-26"), "left_semi")
)

eligible = (
    kept
    .alias("s")
    .join(df_store_sales.alias("x"), (F.col("x.product_id") == F.col("s.id")) & (F.col("x.revenue") <= 1900), "left_anti")
)

joined = (
    eligible
    .alias("e")
    .join(df_suppliers.alias("d"), F.col("e.supplier_id").eqNullSafe(F.col("d.id")), "left")
    .select(
        "e.id",
        "e.product_name",
        "e.stock_qty",
        "e.supplier_id",
        "e.price_avg_2_prior",
        F.col("d.country"),
        F.col("d.rating"),
        F.col("d.city"),
    )
)

base = (
    joined
    .filter(F.col("supplier_id").isNotNull() & F.col("rating").between(2.3, 2.7))
    .select(
        "id",
        "product_name",
        "stock_qty",
        "price_avg_2_prior",
        "country",
        "rating",
        "city",
        F.concat_ws("-", "city", "product_name").alias("city_product_name_key"),
        F.when(F.col("price_avg_2_prior") >= 937, "High").when(F.col("price_avg_2_prior") >= 363, "Medium").otherwise("Low").alias("price_avg_2_prior_level"),
    )
)

enriched = (
    base
    .select(
        "id",
        "product_name",
        "country",
        "rating",
        "price_avg_2_prior_level",
        F.avg("rating").over(w4).alias("rating_avg_5"),
        F.cume_dist().over(w5).alias("stock_qty_cume_dist"),
        F.avg("stock_qty").over(w6).alias("stock_qty_avg_6"),
    )
)

result = (
    enriched
    .groupBy("country")
    .agg(
        F.sort_array(F.collect_set("country")).alias("country_list"),
        F.min("rating_avg_5").alias("min_rating_avg_5"),
        F.size(F.collect_set("country")).alias("num_countries"),
        F.round(F.avg("rating_avg_5"), 2).alias("avg_rating_avg_5"),
    )
    .orderBy(F.col("country").asc_nulls_first())
)
