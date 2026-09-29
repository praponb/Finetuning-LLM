from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country").orderBy("rating", "id")
w2 = Window.partitionBy("country").orderBy(F.col("is_new_york").desc_nulls_first(), "id")
w3 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.cost").desc(), F.col("c.id").desc())
w4 = Window.partitionBy("brand")
w5 = Window.partitionBy("brand").orderBy("product_cost", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.orderBy("prev_rating", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w7 = Window.partitionBy("country").orderBy("brand_stddev_samp_product_cost", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w8 = Window.partitionBy("country").orderBy("event_seq", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

filtered = (
    df_suppliers
    .filter(F.col("city").isNotNull())
    .select(
        "id",
        "country",
        "city",
        "rating",
        F.when(F.col("city") == "New York", 1).otherwise(0).alias("is_new_york"),
    )
)

enriched = (
    filtered
    .filter(F.col("city") == "Berlin")
    .select(
        "*",
        F.lag("rating").over(w).alias("prev_rating"),
        F.lag("is_new_york").over(w).alias("prev_is_new_york"),
        F.lead("is_new_york").over(w).alias("next_is_new_york"),
        F.row_number().over(w).alias("event_seq"),
    )
)

deduped = (
    enriched
    .withColumn("rn", F.row_number().over(w2))
    .filter(F.col("rn") == 1)
)

latest_match = (
    deduped
    .alias("s")
    .join(df_products.alias("c"), (F.col("c.supplier_id") == F.col("s.id")), "left")
    .withColumn("rn_2", F.row_number().over(w3))
    .filter(F.col("rn_2") == 1)
    .select(
        "s.id",
        "s.country",
        "s.rating",
        "s.is_new_york",
        "s.prev_rating",
        "s.prev_is_new_york",
        "s.event_seq",
        "s.rn",
        F.col("c.brand"),
        F.col("c.cost").alias("product_cost"),
    )
)

with_windows = (
    latest_match
    .select(
        "id",
        "country",
        "prev_is_new_york",
        "event_seq",
        F.stddev_samp("product_cost").over(w4).alias("brand_stddev_samp_product_cost"),
        F.sum("is_new_york").over(w5).alias("cumulative_is_new_york"),
        F.sum("is_new_york").over(w6).alias("running_is_new_york"),
    )
)

scored = (
    with_windows
    .select(
        "*",
        F.sum("prev_is_new_york").over(w7).alias("running_sum_prev_is_new_york"),
        F.max("prev_is_new_york").over(w7).alias("running_max_prev_is_new_york"),
        F.count("prev_is_new_york").over(w7).alias("running_count_prev_is_new_york"),
        F.avg("prev_is_new_york").over(w7).alias("running_avg_prev_is_new_york"),
        F.first("prev_is_new_york", ignorenulls=True).over(w8).alias("first_known_prev_is_new_york"),
    )
)

eligible = (
    scored
    .alias("s")
    .join(df_products.alias("c"), (F.col("c.supplier_id") == F.col("s.id")) & (F.col("c.price") > 1150), "left_anti")
)

stats = (
    eligible
    .groupBy(
        F.when(F.col("running_count_prev_is_new_york") >= 13, "HIGH").otherwise("LOW").alias("running_count_prev_is_new_york_tier"),
        F.when(F.col("event_seq") >= 8, "PREMIUM").otherwise("STANDARD").alias("event_seq_tier"),
    )
    .agg(
        F.max(F.when(F.col("country") == "JP", F.col("prev_is_new_york"))).alias("jp_max_prev_is_new_york"),
        F.countDistinct("country").alias("distinct_countries"),
        F.size(F.collect_set("country")).alias("num_countries"),
        F.countDistinct("id").alias("distinct_ids"),
        F.count(F.when(F.col("country") == "US", 1)).alias("us_rows"),
    )
    .filter(F.col("us_rows") < 1000)
)

result = (
    stats
    .select(
        "running_count_prev_is_new_york_tier",
        "event_seq_tier",
        "jp_max_prev_is_new_york",
        "us_rows",
    )
)
