from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city", "country").orderBy(F.col("is_new_york").asc_nulls_first(), "id")
w_running = Window.partitionBy("city").orderBy("rating", "id")
w3 = Window.partitionBy("city").orderBy(F.col("rating_rounded").desc_nulls_first())
w_ordered = Window.partitionBy("city").orderBy("rating_rounded", "id")
w5 = Window.partitionBy("city").orderBy("rating_rounded_rank", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.partitionBy("city").orderBy("next_rating_rounded", "id")
w7 = Window.orderBy("next_rating_rounded", "id")
w8 = Window.partitionBy("city").orderBy("rating_rounded", "id").rowsBetween(-3, -1)

filtered = (
    df_suppliers
    .filter(~F.col("city").eqNullSafe(F.lit("Tokyo")))
    .withColumn("supplier_name_country_key", F.concat_ws("-", "supplier_name", "country"))
    .withColumn("city_filled", F.coalesce("city", F.lit("OTHER")))
)

kept = (
    filtered
    .alias("s")
    .join(df_products.alias("x"), (F.col("x.supplier_id") == F.col("s.id")) & (F.col("x.product_name").like("Yo%")), "left_anti")
)

cleaned = (
    kept
    .filter(F.col("city") == "New York")
    .select(
        "id",
        "supplier_name",
        "country",
        "city",
        "rating",
        "city_filled",
        F.when(F.col("city") == "New York", 1).otherwise(0).alias("is_new_york"),
        F.round("rating", 2).alias("rating_rounded"),
    )
)

eligible = (
    cleaned
    .alias("s")
    .join(df_products.alias("c"), (F.col("c.supplier_id") == F.col("s.id")) & (F.col("c.cost") >= 530), "left_anti")
)

deduped = (
    eligible
    .withColumn("rank_in_group", F.row_number().over(w))
    .filter(F.col("rank_in_group") == 1)
    .select("id", "supplier_name", "city", "rating", "city_filled", "rating_rounded")
)

with_windows = (
    deduped
    .withColumn("rating_bucket", F.ntile(10).over(w_running))
    .withColumn("rating_rounded_rank", F.rank().over(w3))
    .withColumn("row_seq", F.row_number().over(w_running))
    .withColumn("next_rating_rounded", F.lead("rating_rounded", 1).over(w_ordered))
)

enriched = (
    with_windows
    .filter(F.col("row_seq") != 8)
    .withColumn("last_known_rating_rounded_rank", F.last("rating_rounded_rank", ignorenulls=True).over(w5))
    .withColumn("prev_rating_rounded_rank", F.lag("rating_rounded_rank", 2).over(w6))
    .withColumn("next_row_seq", F.lead("row_seq", 2).over(w7))
    .withColumn("rating_bucket_avg_3_prior", F.avg("rating_bucket").over(w8))
    .withColumn("rating_bucket_2", F.ntile(3).over(w_running))
)

result = (
    enriched
    .select(
        "id",
        "rating",
        "city_filled",
        "rating_rounded",
        "rating_bucket",
        "rating_rounded_rank",
        "row_seq",
        "next_rating_rounded",
        "last_known_rating_rounded_rank",
    )
    .orderBy("rating_bucket")
)
