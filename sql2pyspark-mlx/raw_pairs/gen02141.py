from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city").orderBy(F.col("rating").desc_nulls_first())
w2 = Window.partitionBy("city", "country").orderBy(F.col("rating").desc(), "id")
w3 = Window.partitionBy("city")
w4 = Window.partitionBy("country").orderBy("rating", "id")
w5 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.price").desc(), F.col("c.id").desc())
w6 = Window.partitionBy("product_category").orderBy("product_cost", "id").rowsBetween(-6, Window.currentRow)
w7 = Window.partitionBy("product_category").orderBy(F.col("rating_whole").asc_nulls_last())
w_grp = Window.orderBy("rating_whole", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_hist = Window.orderBy("rating_whole", "id")
w10 = Window.orderBy("rating_whole_percent_rank", "id")
w11 = Window.orderBy("event_seq", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w12 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.stock_qty").desc(), F.col("c.id").desc())

base = (
    df_suppliers
    .filter(F.col("rating") < 3.0)
    .withColumn("rating_whole", F.floor("rating"))
    .withColumn("city_group", F.when(F.col("city").isin(["Berlin", "Tokyo"]), "Core").otherwise("Other"))
    .withColumn("resolved_city", F.coalesce("city", F.lit("N/A")))
)

scored = (
    base
    .select(
        "id",
        "rating",
        "rating_whole",
        "city_group",
        F.dense_rank().over(w).alias("rating_dense_rank"),
        F.ntile(4).over(w2).alias("rating_bucket"),
        F.sum("rating").over(w3).alias("city_sum_rating"),
        F.lead("rating_whole", 1).over(w4).alias("next_rating_whole"),
    )
)

with_last = (
    scored
    .alias("s")
    .join(df_products.alias("c"), (F.col("c.supplier_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w5))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.rating",
        "s.rating_whole",
        F.col("c.category").alias("product_category"),
        F.col("c.cost").alias("product_cost"),
    )
)

with_windows = (
    with_last
    .select(
        "id",
        "rating_whole",
        "product_cost",
        F.avg("rating").over(w6).alias("rating_avg_6"),
        F.percent_rank().over(w7).alias("rating_whole_percent_rank"),
    )
)

enriched = (
    with_windows
    .select(
        "id",
        "rating_whole",
        F.sum("rating_avg_6").over(w_grp).alias("cumulative_rating_avg_6"),
        F.row_number().over(w_hist).alias("event_seq"),
        F.ntile(5).over(w10).alias("rating_whole_percent_rank_bucket"),
    )
)

scored_suppliers = (
    enriched
    .withColumn("running_event_seq", F.sum("event_seq").over(w_grp))
    .withColumn("row_seq", F.row_number().over(w_hist))
    .withColumn("last_known_rating_whole", F.last("rating_whole", ignorenulls=True).over(w11))
)

latest_match = (
    scored_suppliers
    .alias("s")
    .join(df_products.alias("c"), (F.col("c.supplier_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w12))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.brand"))
)

result = (
    latest_match
    .groupBy("brand")
    .agg(
        F.round(F.avg("row_seq"), 2).alias("avg_row_seq"),
        F.countDistinct("id").alias("distinct_ids"),
    )
    .orderBy(F.col("brand").desc_nulls_last())
)
