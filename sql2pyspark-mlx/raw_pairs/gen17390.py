from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.stock_qty").desc(), F.col("c.id").desc())

with_latest = (
    df_suppliers
    .alias("s")
    .join(df_products.alias("c"), (F.col("c.supplier_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.brand"))
)

result = (
    with_latest
    .withColumn("rating_tier", F.when(F.col("rating") >= 3.9, "HIGH").otherwise("LOW"))
    .rollup("city", "rating_tier")
    .agg(
        F.percentile_approx("rating", 0.95).alias("rating_p95"),
        F.sum("rating").alias("total_rating"),
        F.round(F.sum("rating") / F.nullif(F.count(F.when(F.col("brand") == "Globex", 1)), F.lit(0)), 2).alias("rating_per_globex"),
        F.sum(F.when(F.col("brand") == "Globex", F.col("rating"))).alias("globex_total_rating"),
        F.count(F.when(F.col("country") == "US", 1)).alias("us_count"),
        F.grouping("city").alias("gid"),
    )
    .filter(F.col("us_count") > 1)
)
