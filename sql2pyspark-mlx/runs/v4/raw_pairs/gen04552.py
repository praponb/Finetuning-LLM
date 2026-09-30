from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.launch_date").desc(), F.col("c.id").desc())
w2 = Window.partitionBy("country").orderBy(F.col("rating").desc_nulls_last())
w3 = Window.partitionBy("city")
w4 = Window.partitionBy("city").orderBy("price", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("country")

with_last = (
    df_suppliers
    .alias("s")
    .join(df_products.alias("c"), (F.col("c.supplier_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.price"))
)

metrics = (
    with_last
    .select(
        "*",
        F.rank().over(w2).alias("rating_rank"),
        F.round((F.col("price") / F.sum("price").over(w3)), 4).alias("price_share"),
        F.sum("rating").over(w4).alias("cumulative_rating"),
    )
)

scored = (
    metrics
    .withColumn("city_max_price", F.max("price").over(w3))
    .withColumn("cumulative_rating_share", F.round((F.col("cumulative_rating") / F.sum("cumulative_rating").over(w5)), 4))
)

not_flagged = (
    scored
    .alias("s")
    .join(df_products.alias("ch"), (F.col("ch.supplier_id") == F.col("s.id")) & (F.col("ch.cost") >= 530), "left_anti")
)

stats = (
    not_flagged
    .rollup("city")
    .agg(
        F.stddev_samp("rating").alias("rating_stddev"),
        F.round(F.avg("cumulative_rating_share"), 2).alias("avg_cumulative_rating_share"),
        F.count(F.when(F.col("country") == "DE", 1)).alias("de_rows"),
        F.count("*").alias("row_count"),
    )
)

result = stats.select("avg_cumulative_rating_share", "row_count")
