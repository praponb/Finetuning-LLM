from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_hist = Window.orderBy("age", "id", "k")
w2 = Window.partitionBy("customer_segment").orderBy("age", "id", "k").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_part = Window.orderBy("age", "id", "k").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_grp = Window.partitionBy("k").orderBy("age", "id", "k").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("k").orderBy("cumulative_age", "id", "k").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.partitionBy("customer_segment").orderBy("cumulative_age_2", "id", "k")
w7 = Window.partitionBy("k", "customer_segment").orderBy("prev_age", "id", "k").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w8 = Window.orderBy(F.col("prev_age").desc(), "id", "k")
w9 = Window.partitionBy("k", "customer_segment")

flattened = df_carts.select("id", "customer_id", F.explode("attributes").alias("k", "v"))

enriched = (
    flattened
    .alias("a")
    .join(df_customers.alias("b"), F.col("a.customer_id") == F.col("b.id"), "left")
    .select("a.*", F.col("b.name"), F.col("b.segment"), F.col("b.country"))
)

enriched_carts = (
    enriched
    .alias("src")
    .join(df_customers.alias("b"), F.col("src.customer_id") == F.col("b.id"), "left")
    .filter(F.col("b.credit_limit").isNotNull())
    .select(
        "src.*",
        F.col("b.name").alias("customer_name"),
        F.col("b.segment").alias("customer_segment"),
        F.col("b.age"),
    )
)

with_windows = (
    enriched_carts
    .select(
        "id",
        "k",
        "v",
        "name",
        "customer_name",
        "customer_segment",
        F.lag("age", 1).over(w_hist).alias("prev_age"),
        F.sum("age").over(w2).alias("cumulative_age"),
        F.first("country", ignorenulls=True).over(w_part).alias("first_known_country"),
        F.last("segment", ignorenulls=True).over(w_grp).alias("last_known_segment"),
        F.sum("age").over(w_grp).alias("cumulative_age_2"),
    )
)

metrics = (
    with_windows
    .select(
        "id",
        "k",
        "customer_segment",
        "prev_age",
        "cumulative_age",
        F.count("cumulative_age").over(w5).alias("running_count_cumulative_age"),
        F.avg("cumulative_age").over(w5).alias("running_avg_cumulative_age"),
        F.min("cumulative_age").over(w5).alias("running_min_cumulative_age"),
        F.max("cumulative_age").over(w5).alias("running_max_cumulative_age"),
        F.lead("cumulative_age", 1).over(w6).alias("next_cumulative_age"),
    )
)

metrics_carts = (
    metrics
    .withColumn("cumulative_cumulative_age", F.sum("cumulative_age").over(w7))
    .withColumn("row_seq", F.row_number().over(w8))
    .withColumn("k_sum_running_max_cumulative_age", F.sum("running_max_cumulative_age").over(w9))
)

totals = (
    metrics_carts
    .cube(
        "k",
        F.when(F.col("next_cumulative_age") >= 304, "LARGE").otherwise("SMALL").alias("next_cumulative_age_tier"),
    )
    .agg(
        F.avg(F.when(F.col("k") == "gift", F.col("next_cumulative_age"))).alias("gift_avg_next_cumulative_age"),
        F.count("*").alias("event_count"),
        F.size(F.collect_set("k")).alias("num_ks"),
    )
    .filter(F.col("num_ks") > 1)
)

result = (
    totals
    .select(
        "k",
        "next_cumulative_age_tier",
        "num_ks",
        F.when(F.col("k").rlike("^(weight_kg|utm_source)$"), "EXTENDED").otherwise("OTHER_ZONE").alias("k_zone"),
    )
    .orderBy("k_zone")
)
