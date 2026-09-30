from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country")
w2 = Window.orderBy("rating", "id").rowsBetween(-2, Window.currentRow)
w3 = Window.partitionBy("city").orderBy(F.col("rating").desc_nulls_first())
w4 = Window.orderBy(F.col("rating").desc(), "id")
w5 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.stock_qty").desc(), F.col("c.id").desc())
w6 = Window.partitionBy("country").orderBy("cost", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w7 = Window.partitionBy("country").orderBy(F.col("cost").desc(), "id")
w8 = Window.partitionBy("country").orderBy("rating_avg_2", "id")
w9 = Window.partitionBy("country").orderBy("rating_dense_rank", "id")

valid = (
    df_suppliers
    .filter(F.col("city").isNotNull() & (F.col("rating") > 3.7))
    .withColumn("city_prefix", F.substring("city", 1, 4))
)

scored = (
    valid
    .select(
        "*",
        F.avg("rating").over(w).alias("country_avg_rating"),
        F.avg("rating").over(w2).alias("rating_avg_2"),
        F.dense_rank().over(w3).alias("rating_dense_rank"),
        F.row_number().over(w4).alias("seq_no"),
    )
)

with_latest = (
    scored
    .alias("s")
    .join(df_products.alias("c"), (F.col("c.supplier_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w5))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.cost"))
)

metrics = (
    with_latest
    .filter(F.col("city") != "Chiang Mai")
    .select(
        "*",
        F.sum("rating_avg_2").over(w6).alias("cumulative_rating_avg_2"),
        F.ntile(5).over(w7).alias("cost_bucket"),
        F.ntile(3).over(w8).alias("rating_avg_2_bucket"),
        F.lag("cost", 1).over(w9).alias("prev_cost"),
    )
)

result = (
    metrics
    .groupBy(
        "country",
        "seq_no",
        F.when(F.col("rating_avg_2") >= 4, "HIGH").otherwise("LOW").alias("rating_avg_2_tier"),
    )
    .agg(
        F.round(F.sum("rating_avg_2") / F.nullif(F.count(F.when(F.col("city") == "Singapore", 1)), F.lit(0)), 2).alias("rating_avg_2_per_singapore"),
        F.count(F.when(F.col("country") == "JP", 1)).alias("jp_count"),
        F.count(F.when(F.col("city") == "Chiang Mai", 1)).alias("chiang_mai_count"),
    )
    .orderBy("jp_count")
)
