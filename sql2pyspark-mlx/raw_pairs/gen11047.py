from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("rating").asc_nulls_first())
w2 = Window.partitionBy("country").orderBy("rating", "id").rowsBetween(-2, -1)
w3 = Window.partitionBy("country").orderBy("rating", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.cost").desc(), F.col("c.id").desc())
w5 = Window.orderBy("product_price", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.orderBy(F.col("rating_rank").desc_nulls_first())
w7 = Window.orderBy("cost", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w8 = Window.orderBy(F.col("cost").desc())
w9 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.launch_date").desc(), F.col("c.id").desc())

valid = (
    df_suppliers
    .filter(F.col("city").isNotNull() & (F.col("city").eqNullSafe(F.lit("London"))))
    .select(
        "id",
        "country",
        "rating",
        F.coalesce("supplier_name", F.lit("N/A")).alias("supplier_name_filled"),
        F.lower("city").alias("city_lower"),
        F.coalesce("city", F.lit("OTHER")).alias("city_clean"),
    )
)

metrics = (
    valid
    .select(
        "id",
        "supplier_name_filled",
        "city_lower",
        "city_clean",
        F.rank().over(w).alias("rating_rank"),
        F.avg("rating").over(w2).alias("rating_avg_2_prior"),
        F.sum("rating").over(w3).alias("cumulative_rating"),
    )
)

with_last = (
    metrics
    .alias("s")
    .join(df_products.alias("c"), (F.col("c.supplier_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w4))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.city_clean",
        "s.rating_rank",
        "s.rating_avg_2_prior",
        "s.cumulative_rating",
        F.col("c.price").alias("product_price"),
        F.col("c.cost"),
    )
)

enriched = (
    with_last
    .withColumn("last_known_rating_avg_2_prior", F.last("rating_avg_2_prior", ignorenulls=True).over(w5))
    .withColumn("rating_rank_dense_rank", F.dense_rank().over(w6))
    .withColumn("cumulative_cumulative_rating", F.sum("cumulative_rating").over(w7))
    .withColumn("cost_dense_rank", F.dense_rank().over(w8))
)

latest_match = (
    enriched
    .alias("s")
    .join(df_products.alias("c"), (F.col("c.supplier_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w9))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.rating_avg_2_prior",
        "s.cumulative_rating",
        "s.last_known_rating_avg_2_prior",
        "s.rating_rank_dense_rank",
        "s.cumulative_cumulative_rating",
        "s.cost_dense_rank",
        F.col("c.price").alias("product_price_2"),
    )
)

result = (
    latest_match
    .select(
        "id",
        "cumulative_rating",
        "last_known_rating_avg_2_prior",
        "rating_rank_dense_rank",
        "cumulative_cumulative_rating",
        "cost_dense_rank",
        "product_price_2",
    )
)
