from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_running = Window.partitionBy("city").orderBy("rating", "id")
w2 = Window.partitionBy("country").orderBy("rating", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("country").orderBy("event_seq", "id")
w4 = Window.partitionBy("country").orderBy("running_rating", "id")
w5 = Window.partitionBy("country").orderBy("event_seq", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.partitionBy("country").orderBy("prev_rating", "id")

scored = (
    df_suppliers
    .select(
        "id",
        "country",
        "rating",
        F.lead("rating").over(w_running).alias("next_rating"),
        F.row_number().over(w_running).alias("event_seq"),
        F.sum("rating").over(w2).alias("running_rating"),
    )
)

enriched = (
    scored
    .select(
        "*",
        F.lead("rating").over(w3).alias("next_rating_2"),
        F.lag("rating").over(w3).alias("prev_rating"),
        F.lag("running_rating").over(w3).alias("prev_running_rating"),
        F.row_number().over(w3).alias("row_seq"),
        F.ntile(10).over(w4).alias("running_rating_bucket"),
    )
)

filtered = (
    enriched
    .filter(F.col("prev_running_rating").between(8, 18))
    .select(
        "id",
        "country",
        "rating",
        "event_seq",
        "next_rating_2",
        "prev_rating",
        "prev_running_rating",
        "row_seq",
        "running_rating_bucket",
        (F.col("next_rating") - F.col("prev_rating")).alias("next_rating_minus_prev_rating"),
        F.greatest("prev_rating", "running_rating").alias("greatest_prev_rating_running_rating"),
    )
)

not_flagged = (
    filtered
    .alias("s")
    .join(df_products.alias("c"), (F.col("c.supplier_id") == F.col("s.id")) & (F.col("c.supplier_id").isNotNull()), "left_anti")
)

scored_suppliers = (
    not_flagged
    .filter(F.col("prev_running_rating").between(13, 17))
    .select(
        "*",
        F.sum("next_rating_minus_prev_rating").over(w5).alias("running_sum_next_rating_minus_prev_rating"),
        F.min("next_rating_minus_prev_rating").over(w5).alias("running_min_next_rating_minus_prev_rating"),
        F.count("next_rating_minus_prev_rating").over(w5).alias("running_count_next_rating_minus_prev_rating"),
        F.row_number().over(w6).alias("event_seq_2"),
    )
)

result = (
    scored_suppliers
    .groupBy(
        F.when(F.col("event_seq_2") >= 9, "LARGE").otherwise("SMALL").alias("event_seq_2_tier"),
        F.when(F.col("prev_rating") >= 3.9, "PREMIUM").otherwise("STANDARD").alias("prev_rating_tier"),
    )
    .agg(
        F.countDistinct("country").alias("distinct_countries"),
        F.count_if(F.col("country") == "TH").alias("th_events"),
        F.sum(F.when(F.col("country") == "UK", F.col("row_seq"))).alias("uk_total_row_seq"),
        F.count(F.when(F.col("country") == "US", 1)).alias("us_count"),
        F.percentile_approx("next_rating_minus_prev_rating", [0.25, 0.5, 0.75]).alias("next_rating_minus_prev_rating_quartiles"),
        F.round(F.sum("running_sum_next_rating_minus_prev_rating") / F.nullif(F.count(F.when(F.col("country") == "JP", 1)), F.lit(0)), 2).alias("running_sum_next_rating_minus_prev_rating_per_jp"),
    )
    .filter(F.col("running_sum_next_rating_minus_prev_rating_per_jp") >= 0)
    .orderBy(F.col("uk_total_row_seq").desc())
)
