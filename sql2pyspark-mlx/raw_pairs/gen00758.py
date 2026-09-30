from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city").orderBy(F.col("rating").desc(), "id")
w2 = Window.partitionBy("country").orderBy("rating", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("country").orderBy("rating", "id").rowsBetween(-6, Window.currentRow)
w4 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.cost").desc(), F.col("c.id").desc())
w5 = Window.partitionBy("city").orderBy(F.col("rating").asc_nulls_last(), "id")

deduped = (
    df_suppliers
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

metrics = (
    deduped
    .withColumn("last_known_rating", F.last("rating", ignorenulls=True).over(w2))
    .withColumn("rating_avg_6", F.avg("rating").over(w3))
)

with_last = (
    metrics
    .alias("s")
    .join(df_products.alias("c"), (F.col("c.supplier_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w4))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.city",
        "s.rating",
        "s.last_known_rating",
        "s.rating_avg_6",
        F.col("c.cost"),
        F.col("c.price").alias("product_price"),
    )
)

base = (
    with_last
    .filter(F.col("city").isNotNull())
    .select(
        "id",
        "city",
        "rating",
        "rating_avg_6",
        "cost",
        "product_price",
        F.round((F.col("product_price") / F.col("rating_avg_6")), 2).alias("product_price_per_rating_avg_6"),
        (F.col("last_known_rating") - F.col("product_price")).alias("last_known_rating_minus_product_price"),
    )
)

deduped_suppliers = (
    base
    .withColumn("rn", F.row_number().over(w5))
    .filter(F.col("rn") == 1)
)

eligible = (
    deduped_suppliers
    .alias("s")
    .join(df_products.alias("x"), (F.col("x.supplier_id") == F.col("s.id")) & (F.size(F.col("x.tags")) > 2), "left_anti")
)

filtered = (
    eligible
    .filter(F.col("rating") <= 3.0)
    .withColumn("rating_avg_6_filled", F.coalesce("rating_avg_6", F.lit(0)))
)

result = (
    filtered
    .groupBy("city")
    .agg(
        F.sum(F.when(F.col("city") == "Chiang Mai", F.col("cost"))).alias("chiang_mai_total_cost"),
        F.count("*").alias("cnt"),
        F.count(F.when(F.col("city") == "Tokyo", 1)).alias("tokyo_count"),
        F.count(F.when(F.col("city") == "Singapore", 1)).alias("singapore_count"),
        F.sort_array(F.collect_set("city")).alias("city_list"),
        F.count_if(F.col("city") == "London").alias("london_events"),
    )
    .filter(F.col("cnt") < 1000)
    .orderBy(F.col("singapore_count").desc_nulls_first())
)
