from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country").orderBy(F.col("rating").desc(), "id")
w_running = Window.partitionBy("city").orderBy("rating", "id").rowsBetween(-3, Window.currentRow)
w_grp = Window.partitionBy("country", "city").orderBy("rating", "id")
w4 = Window.partitionBy("city").orderBy("last4_max_rating", "id")
w5 = Window.partitionBy("city")
w6 = Window.partitionBy("city").orderBy("prev_last4_max_rating", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w7 = Window.orderBy("prev_last4_avg_rating", "id")

filtered = (
    df_suppliers
    .filter(F.upper("supplier_name").like("%URO%"))
    .withColumn("clean_supplier_name", F.trim("supplier_name"))
)

deduped = (
    filtered
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
    .select("id", "country", "city", "rating", "clean_supplier_name")
)

scored = (
    deduped
    .select(
        "*",
        F.min("rating").over(w_running).alias("last4_min_rating"),
        F.avg("rating").over(w_running).alias("last4_avg_rating"),
        F.count("rating").over(w_running).alias("last4_count_rating"),
        F.max("rating").over(w_running).alias("last4_max_rating"),
        F.lead("rating", 1).over(w_grp).alias("next_rating"),
    )
)

enriched = (
    scored
    .select(
        "id",
        "city",
        "last4_max_rating",
        "next_rating",
        F.lag("last4_max_rating").over(w4).alias("prev_last4_max_rating"),
        F.lead("last4_max_rating").over(w4).alias("next_last4_max_rating"),
        F.lag("last4_avg_rating").over(w4).alias("prev_last4_avg_rating"),
        F.row_number().over(w4).alias("row_seq"),
    )
)

filtered_suppliers = (
    enriched
    .filter(F.col("city").isNotNull() & (~F.col("city").eqNullSafe(F.lit("Chiang Mai"))))
    .select(
        "id",
        "city",
        "last4_max_rating",
        "next_rating",
        "prev_last4_max_rating",
        "prev_last4_avg_rating",
        F.round("row_seq", 0).alias("row_seq_rounded"),
        (F.col("row_seq") * F.col("next_rating")).alias("row_seq_times_next_rating"),
    )
)

with_windows = (
    filtered_suppliers
    .withColumn("city_count_next_rating", F.count("next_rating").over(w5))
    .withColumn("first_known_prev_last4_max_rating", F.first("prev_last4_max_rating", ignorenulls=True).over(w6))
    .withColumn("next_row_seq_times_next_rating", F.lead("row_seq_times_next_rating", 1).over(w7))
)

result = (
    with_windows
    .groupBy(
        "city",
        F.when(F.col("next_row_seq_times_next_rating") >= 73, "PREMIUM").otherwise("STANDARD").alias("next_row_seq_times_next_rating_tier"),
    )
    .agg(
        F.count_if(F.col("city") == "Bangkok").alias("bangkok_events"),
        F.count("*").alias("event_count"),
        F.min("row_seq_rounded").alias("min_row_seq_rounded"),
    )
    .filter(F.col("min_row_seq_rounded") > 0)
    .orderBy(F.col("city").asc_nulls_last())
)
