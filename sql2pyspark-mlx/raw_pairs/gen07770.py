from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city")
w2 = Window.partitionBy("city").orderBy("rating", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_grp = Window.partitionBy("city").orderBy(F.col("cumulative_rating").asc_nulls_last())
w4 = Window.partitionBy("country", "city").orderBy(F.col("rating_share").desc(), "id")
w5 = Window.partitionBy("country", "city").orderBy("rating", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_running = Window.partitionBy("city").orderBy("rating", "id")
w7 = Window.partitionBy("country", "city").orderBy("cumulative_rating_cume_dist", "id")

metrics = (
    df_suppliers
    .withColumn("rating_share", F.round((F.col("rating") / F.sum("rating").over(w)), 4))
    .withColumn("cumulative_rating", F.sum("rating").over(w2))
)

scored = (
    metrics
    .withColumn("cumulative_rating_cume_dist", F.cume_dist().over(w_grp))
    .withColumn("rating_share_bucket", F.ntile(3).over(w4))
    .withColumn("cumulative_cumulative_rating", F.sum("cumulative_rating").over(w5))
    .withColumn("prev_rating", F.lag("rating", 2).over(w_running))
)

latest = (
    scored
    .withColumn("rn", F.row_number().over(w7))
    .filter(F.col("rn") == 1)
)

eligible = (
    latest
    .alias("s")
    .join(df_products.alias("c"), (F.col("c.supplier_id") == F.col("s.id")) & (F.col("c.supplier_id").isNotNull()), "left_anti")
)

kept = (
    eligible
    .alias("s")
    .join(df_products.alias("c"), (F.col("c.supplier_id") == F.col("s.id")) & (F.col("c.category") != "Grocery"), "left_anti")
)

kept_suppliers = (
    kept
    .alias("s")
    .join(df_products.alias("c"), (F.col("c.supplier_id") == F.col("s.id")) & (F.col("c.category") != "Grocery"), "left_anti")
)

result = (
    kept_suppliers
    .withColumn("rating_share_tier", F.when(F.col("rating_share") >= 0.61, "LARGE").otherwise("SMALL"))
    .rollup("rating_share_tier", "city")
    .agg(
        F.round(F.sum("cumulative_cumulative_rating") / F.nullif(F.count(F.when(F.col("city") == "Berlin", 1)), F.lit(0)), 2).alias("cumulative_cumulative_rating_per_berlin"),
        F.count_if(F.col("country") == "DE").alias("de_events"),
        F.count(F.when(F.col("city") == "Bangkok", 1)).alias("bangkok_count"),
        F.size(F.collect_set("country")).alias("num_countries"),
        F.grouping_id().alias("grouping_id"),
    )
    .orderBy(F.col("city").desc_nulls_first())
)
