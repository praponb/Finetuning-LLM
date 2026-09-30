from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.cost").desc(), F.col("c.id").desc())
w2 = Window.partitionBy("city", "product_category").orderBy(F.col("rating").desc())
w3 = Window.partitionBy("product_category")
w4 = Window.orderBy("rating", "id")
w5 = Window.partitionBy("city")
w6 = Window.partitionBy("product_category", "country")

latest_match = (
    df_suppliers
    .alias("s")
    .join(df_products.alias("c"), (F.col("c.supplier_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.price"), F.col("c.category").alias("product_category"))
)

latest = (
    latest_match
    .withColumn("row_num", F.rank().over(w2))
    .filter(F.col("row_num") == 1)
)

not_flagged = (
    latest
    .alias("s")
    .join(df_products.alias("ch"), (F.col("ch.supplier_id") == F.col("s.id")) & (F.col("ch.supplier_id").isNull()), "left_anti")
)

eligible = (
    not_flagged
    .alias("s")
    .join(df_products.alias("c"), (F.col("c.supplier_id") == F.col("s.id")) & (F.col("c.launch_date") > "2024-10-01"), "left_semi")
)

kept = (
    eligible
    .alias("s")
    .join(df_products.alias("c"), (F.col("c.supplier_id") == F.col("s.id")) & (F.array_contains(F.col("c.tags"), "clearance")), "left_anti")
)

enriched = (
    kept
    .select(
        "*",
        F.stddev_samp("price").over(w3).alias("product_category_stddev_samp_price"),
        F.lag("price", 1).over(w4).alias("prev_price"),
        F.avg("rating").over(w5).alias("city_avg_rating"),
        F.round((F.col("price") / F.sum("price").over(w6)), 4).alias("price_share"),
    )
)

result = (
    enriched
    .groupBy("product_category")
    .agg(
        F.percentile_approx("price_share", [0.25, 0.5, 0.75]).alias("price_share_quartiles"),
        F.size(F.collect_set("country")).alias("num_countries"),
        F.avg(F.when(F.col("city") == "Singapore", F.col("price"))).alias("singapore_avg_price"),
        F.percentile_approx("city_avg_rating", 0.9).alias("city_avg_rating_p90"),
    )
    .filter(F.col("singapore_avg_price") > 0)
)
