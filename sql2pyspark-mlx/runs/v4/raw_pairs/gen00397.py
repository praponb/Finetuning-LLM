from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("rating", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("city")
w_recent = Window.partitionBy("country").orderBy("rating", "id")
w4 = Window.partitionBy("country").orderBy("row_seq_whole", "id")
w5 = Window.partitionBy("country").orderBy(F.col("running_rating").asc_nulls_first())
w6 = Window.partitionBy("country").orderBy(F.col("row_seq_whole").desc(), "id")
w7 = Window.partitionBy("country").orderBy("row_seq", "id").rowsBetween(-5, -1)
w8 = Window.orderBy("row_seq_whole", "id").rowsBetween(-3, -1)
w9 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.cost").desc(), F.col("c.id").desc())

eligible = (
    df_suppliers
    .alias("s")
    .join(df_products.alias("x"), (F.col("x.supplier_id") == F.col("s.id")) & (F.array_contains(F.col("x.tags"), "new")), "left_semi")
)

enriched = (
    eligible
    .withColumn("running_rating", F.sum("rating").over(w))
    .withColumn("rating_share", F.round((F.col("rating") / F.sum("rating").over(w2)), 4))
    .withColumn("city_sum_rating", F.sum("rating").over(w2))
    .withColumn("row_seq", F.row_number().over(w_recent))
)

cleaned = (
    enriched
    .filter(F.col("city").isNotNull() & (F.col("city_sum_rating") >= 11))
    .select(
        "id",
        "supplier_name",
        "country",
        "rating",
        "running_rating",
        "rating_share",
        "city_sum_rating",
        "row_seq",
        F.ceil("row_seq").alias("row_seq_whole"),
        F.coalesce("city", F.lit("OTHER")).alias("resolved_city"),
    )
)

eligible_suppliers = (
    cleaned
    .alias("s")
    .join(df_products.alias("ch"), (F.col("ch.supplier_id") == F.col("s.id")) & (F.col("ch.product_name").like("%Chai%")), "left_anti")
)

metrics = (
    eligible_suppliers
    .withColumn("seq_no", F.row_number().over(w4))
    .withColumn("running_rating_percent_rank", F.percent_rank().over(w5))
    .withColumn("row_seq_whole_bucket", F.ntile(3).over(w6))
    .withColumn("running_rating_avg_5_prior", F.avg("running_rating").over(w7))
    .withColumn("row_seq_whole_avg_3_prior", F.avg("row_seq_whole").over(w8))
)

latest_match = (
    metrics
    .alias("s")
    .join(df_products.alias("c"), (F.col("c.supplier_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w9))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.brand").alias("product_brand"))
)

stats = (
    latest_match
    .groupBy("seq_no", "product_brand")
    .agg(
        F.sort_array(F.collect_set("country")).alias("country_list"),
        F.count("*").alias("event_count"),
    )
    .filter(F.col("event_count") < 1)
)

result = stats.select("seq_no", "product_brand", "country_list")
