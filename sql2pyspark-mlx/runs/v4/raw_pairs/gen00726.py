from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.cost").desc(), F.col("c.id").desc())
w_hist = Window.orderBy(F.col("rating").desc())
w3 = Window.partitionBy("city", "country")
w_part = Window.partitionBy("category", "city").orderBy(F.col("rating").asc_nulls_last())
w5 = Window.partitionBy("category")
w6 = Window.partitionBy("city")
w7 = Window.partitionBy("country").orderBy("city_count_cost")

latest_match = (
    df_suppliers
    .alias("s")
    .join(df_products.alias("c"), (F.col("c.supplier_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.category"), F.col("c.cost"))
)

eligible = (
    latest_match
    .alias("s")
    .join(df_products.alias("x"), (F.col("x.supplier_id") == F.col("s.id")) & (F.size(F.col("x.tags")) >= 1), "left_anti")
)

matched_keys = (
    eligible
    .select("city", "id")
    .exceptAll(
        df_suppliers
        .filter(F.col("country").isin(["UK", "US", "DE"]))
        .select("city", "id")
    )
)

reconciled = (
    eligible
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.city") == F.col("k.city")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

scored = (
    reconciled
    .withColumn("rating_cume_dist", F.cume_dist().over(w_hist))
    .withColumn("city_count_cost", F.count("cost").over(w3))
    .withColumn("rating_dense_rank", F.dense_rank().over(w_part))
)

scored_suppliers = (
    scored
    .withColumn("rating_dense_rank_share", F.round((F.col("rating_dense_rank") / F.sum("rating_dense_rank").over(w5)), 4))
    .withColumn("city_count_cost_share", F.round((F.col("city_count_cost") / F.sum("city_count_cost").over(w6)), 4))
    .withColumn("city_count_cost_rank", F.rank().over(w7))
)

stats = (
    scored_suppliers
    .groupBy("city")
    .agg(
        F.percentile_approx("city_count_cost_share", [0.25, 0.5, 0.75]).alias("city_count_cost_share_quartiles"),
        F.round(F.avg("rating_dense_rank_share"), 2).alias("avg_rating_dense_rank_share"),
        F.sum(F.when(F.col("city") == "Singapore", F.col("rating_dense_rank_share")).otherwise(0)).alias("singapore_rating_dense_rank_share"),
        F.round(F.avg("city_count_cost_rank"), 2).alias("avg_city_count_cost_rank"),
        F.size(F.collect_set("city")).alias("num_cities"),
        F.count_if(F.col("country") == "UK").alias("uk_events"),
    )
)

result = (
    stats
    .select(
        "city",
        "avg_rating_dense_rank_share",
        "avg_city_count_cost_rank",
        "num_cities",
        "uk_events",
    )
)
