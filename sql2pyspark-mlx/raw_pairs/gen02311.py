from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.stock_qty").desc(), F.col("c.id").desc())
w_running = Window.partitionBy("city").orderBy("rating", "id")
w3 = Window.orderBy("seq_no", "id")
w4 = Window.orderBy(F.col("seq_no").desc(), "id")
w5 = Window.orderBy(F.col("prev_rating").desc_nulls_last())
w6 = Window.orderBy("prev_price_whole", "id")

base = (
    df_suppliers
    .filter(F.col("country").isin(["US", "TH", "DE"]))
    .select(
        "id",
        "supplier_name",
        "country",
        "city",
        "rating",
        F.length("city").alias("city_length"),
        F.upper("country").alias("country_upper"),
    )
)

with_last = (
    base
    .alias("s")
    .join(df_products.alias("c"), (F.col("c.supplier_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.id", "s.city", "s.rating", "s.country_upper", F.col("c.price"))
)

enriched = (
    with_last
    .select(
        "id",
        "country_upper",
        "price",
        F.lag("price").over(w_running).alias("prev_price"),
        F.lag("rating").over(w_running).alias("prev_rating"),
        F.row_number().over(w_running).alias("seq_no"),
    )
)

not_flagged = (
    enriched
    .alias("s")
    .join(df_products.alias("x"), (F.col("x.supplier_id") == F.col("s.id")) & (F.col("x.launch_date").between("2025-06-27", "2025-07-01")), "left_semi")
)

with_windows = (
    not_flagged
    .filter(F.col("prev_rating") > 3.6)
    .withColumn("next_prev_rating", F.lead("prev_rating").over(w3))
    .withColumn("prev_prev_rating", F.lag("prev_rating").over(w3))
    .withColumn("prev_price_2", F.lag("price").over(w3))
    .withColumn("next_price", F.lead("price").over(w3))
    .withColumn("event_seq", F.row_number().over(w3))
)

cleaned = (
    with_windows
    .filter(F.col("prev_price").between(770, 1110))
    .withColumn("prev_price_whole", F.floor("prev_price"))
)

scored = (
    cleaned
    .select(
        "*",
        F.ntile(5).over(w4).alias("seq_no_bucket"),
        F.cume_dist().over(w5).alias("prev_rating_cume_dist"),
        F.ntile(5).over(w6).alias("prev_price_whole_bucket"),
    )
)

grouped = (
    scored
    .withColumn("prev_price_tier", F.when(F.col("prev_price") >= 1200, "HIGH").otherwise("LOW"))
    .withColumn("next_prev_rating_tier", F.when(F.col("next_prev_rating") >= 4.4, "LARGE").otherwise("SMALL"))
    .groupingSets(
        [["prev_price_tier", "next_prev_rating_tier"], ["prev_price_tier"], []],
        "prev_price_tier",
        "next_prev_rating_tier",
    )
    .agg(
        F.round(F.avg("prev_price"), 2).alias("avg_prev_price"),
        F.avg("prev_price_whole_bucket").alias("avg_prev_price_whole_bucket"),
        F.count("*").alias("num_rows"),
        F.percentile_approx("event_seq", [0.25, 0.5, 0.75]).alias("event_seq_quartiles"),
        F.max("seq_no").alias("max_seq_no"),
        F.percentile_approx("prev_price_2", [0.25, 0.5, 0.75]).alias("prev_price_2_quartiles"),
        F.grouping("prev_price_tier").alias("grouping_level"),
    )
)

result = (
    grouped
    .select(
        "prev_price_tier",
        "next_prev_rating_tier",
        "avg_prev_price",
        "avg_prev_price_whole_bucket",
        "num_rows",
        "event_seq_quartiles",
        "max_seq_no",
        "prev_price_2_quartiles",
    )
)
