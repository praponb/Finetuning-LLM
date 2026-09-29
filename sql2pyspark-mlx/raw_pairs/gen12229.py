from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.price").desc(), F.col("c.id").desc())
w2 = Window.partitionBy("price_tier").orderBy(F.col("avg_price").desc())

with_latest = (
    df_suppliers
    .alias("s")
    .join(df_products.alias("c"), (F.col("c.supplier_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.id", "s.rating", F.col("c.price"))
)

result = (
    with_latest
    .groupBy(
        F.when(F.col("price") >= 970, "HIGH").otherwise("LOW").alias("price_tier"),
        F.when(F.col("rating") >= 3.7, "PREMIUM").otherwise("STANDARD").alias("rating_tier"),
    )
    .agg(
        F.countDistinct("id").alias("distinct_ids"),
        F.round(F.avg("price"), 2).alias("avg_price"),
        F.stddev_samp("rating").alias("rating_stddev"),
        F.percentile_approx("rating", 0.5).alias("rating_p50"),
    )
    .withColumn("avg_price_dense_rank", F.dense_rank().over(w2))
    .orderBy(F.col("distinct_ids").desc())
)
