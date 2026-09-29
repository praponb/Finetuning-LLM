from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country").orderBy("rating", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("city", "country").orderBy(F.col("rating_adjusted").desc(), "id")
w3 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.stock_qty").desc(), F.col("c.id").desc())
w4 = Window.orderBy("rating_adjusted", "id")
w5 = Window.orderBy("rating_adjusted_2", "id")
w6 = Window.orderBy("rating", "id")
w7 = Window.partitionBy("city", "brand").orderBy(F.col("price").desc_nulls_first())
w8 = Window.partitionBy("city").orderBy(F.col("rating_adjusted_2").asc_nulls_first())

base = (
    df_suppliers
    .filter(F.col("city").isNotNull())
    .select(
        "id",
        "supplier_name",
        "country",
        "city",
        "rating",
        F.round((F.col("rating") * 0.85), 2).alias("rating_adjusted"),
        F.concat("city", F.lit("/"), "country").alias("city_country"),
    )
)

enriched = (
    base
    .select(
        "id",
        "supplier_name",
        "city",
        "rating",
        "rating_adjusted",
        "city_country",
        F.avg("rating_adjusted").over(w).alias("running_avg_rating_adjusted"),
        F.max("rating_adjusted").over(w).alias("running_max_rating_adjusted"),
        F.min("rating_adjusted").over(w).alias("running_min_rating_adjusted"),
        F.ntile(10).over(w2).alias("rating_adjusted_bucket"),
    )
)

filtered = (
    enriched
    .filter(F.col("city").isNotNull())
    .withColumn("rating_adjusted_2", F.round((F.col("rating") * 1.15), 2))
)

with_latest = (
    filtered
    .alias("s")
    .join(df_products.alias("c"), (F.col("c.supplier_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w3))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.city",
        "s.rating",
        "s.rating_adjusted",
        "s.running_max_rating_adjusted",
        "s.running_min_rating_adjusted",
        "s.rating_adjusted_bucket",
        "s.rating_adjusted_2",
        F.col("c.brand"),
        F.col("c.price"),
    )
)

result = (
    with_latest
    .withColumn("prev_price", F.lag("price", 2).over(w4))
    .withColumn("row_seq", F.row_number().over(w5))
    .withColumn("prev_running_max_rating_adjusted", F.lag("running_max_rating_adjusted", 2).over(w6))
    .withColumn("price_cume_dist", F.cume_dist().over(w7))
    .withColumn("rating_adjusted_2_dense_rank", F.dense_rank().over(w8))
)
