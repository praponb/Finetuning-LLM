from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("rating").desc_nulls_last())
w2 = Window.orderBy("rating", "id")
w3 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.cost").desc(), F.col("c.id").desc())
w4 = Window.partitionBy("city").orderBy(F.col("rating_cume_dist").desc_nulls_last(), "id")

with_windows = (
    df_suppliers
    .filter(F.col("city") != "Tokyo")
    .select(
        "id",
        "supplier_name",
        "city",
        "rating",
        F.cume_dist().over(w).alias("rating_cume_dist"),
        F.ntile(10).over(w2).alias("rating_bucket"),
    )
)

eligible = (
    with_windows
    .alias("s")
    .join(df_products.alias("ch"), (F.col("ch.supplier_id") == F.col("s.id")) & (F.size(F.col("ch.tags")) >= 2), "left_anti")
)

not_flagged = (
    eligible
    .alias("s")
    .join(df_products.alias("ch"), (F.col("ch.supplier_id") == F.col("s.id")) & (F.col("ch.price").between(950, 1020)), "left_anti")
)

with_latest = (
    not_flagged
    .alias("s")
    .join(df_products.alias("c"), (F.col("c.supplier_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w3))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.cost"))
)

ranked = (
    with_latest
    .withColumn("rn", F.row_number().over(w4))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

result = (
    ranked
    .withColumn("cost_tier", F.when(F.col("cost") >= 720, "LARGE").otherwise("SMALL"))
    .groupingSets([["cost_tier", "city"], ["cost_tier"], []], "cost_tier", "city")
    .agg(
        F.round(F.avg("cost"), 2).alias("avg_cost"),
        F.sort_array(F.collect_set("city")).alias("city_list"),
        F.sum(F.when(F.col("city") == "Berlin", F.col("rating"))).alias("berlin_total_rating"),
        F.count_if(F.col("city") == "Bangkok").alias("bangkok_events"),
        F.round(F.sum("rating_cume_dist") / F.nullif(F.count(F.when(F.col("city") == "Chiang Mai", 1)), F.lit(0)), 2).alias("rating_cume_dist_per_chiang_mai"),
        F.grouping("cost_tier").alias("gid"),
    )
    .orderBy("berlin_total_rating")
)
