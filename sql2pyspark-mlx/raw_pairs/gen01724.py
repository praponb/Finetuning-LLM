from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city").orderBy("rating", "id")
w2 = Window.partitionBy("city")
w3 = Window.partitionBy("city").orderBy("seq_no", "id")
w4 = Window.orderBy("next_city_stddev_samp_rating", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("city").orderBy(F.col("seq_no").desc(), "id")
w6 = Window.partitionBy("city").orderBy(F.col("seq_no").desc_nulls_first())
w7 = Window.partitionBy("city").orderBy(F.col("next_city_stddev_samp_rating").desc(), "id")
w8 = Window.partitionBy("city").orderBy(F.col("prev_seq_no").desc_nulls_first())
w9 = Window.partitionBy("city").orderBy("seq_no_dense_rank", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

filtered = (
    df_suppliers
    .filter(~F.col("city").eqNullSafe(F.lit("Bangkok")))
    .withColumn("supplier_name_filled", F.coalesce("supplier_name", F.lit("Unknown")))
    .withColumn("resolved_city", F.coalesce("city", F.lit("OTHER")))
)

not_flagged = (
    filtered
    .alias("s")
    .join(df_products.alias("ch"), (F.col("ch.supplier_id") == F.col("s.id")) & (F.col("ch.launch_date") >= "2024-12-01"), "left_anti")
)

scored = (
    not_flagged
    .select(
        "id",
        "supplier_name",
        "city",
        "supplier_name_filled",
        F.row_number().over(w).alias("seq_no"),
        F.stddev_samp("rating").over(w2).alias("city_stddev_samp_rating"),
    )
)

kept = (
    scored
    .alias("s")
    .join(df_products.alias("x"), (F.col("x.supplier_id") == F.col("s.id")) & (F.col("x.cost").between(250, 360)), "left_anti")
)

not_flagged_suppliers = (
    kept
    .alias("s")
    .join(df_products.alias("c"), (F.col("c.supplier_id") == F.col("s.id")) & (F.col("c.brand") == "Stark"), "left_anti")
)

metrics = (
    not_flagged_suppliers
    .select(
        "*",
        F.lead("city_stddev_samp_rating").over(w3).alias("next_city_stddev_samp_rating"),
        F.lag("city_stddev_samp_rating").over(w3).alias("prev_city_stddev_samp_rating"),
        F.lag("seq_no").over(w3).alias("prev_seq_no"),
        F.lead("seq_no").over(w3).alias("next_seq_no"),
        F.row_number().over(w3).alias("event_seq"),
    )
)

enriched = (
    metrics
    .select(
        "*",
        F.sum("event_seq").over(w4).alias("cumulative_event_seq"),
        F.ntile(5).over(w5).alias("seq_no_bucket"),
        F.dense_rank().over(w6).alias("seq_no_dense_rank"),
    )
)

result = (
    enriched
    .select(
        "id",
        "supplier_name_filled",
        "city_stddev_samp_rating",
        "next_city_stddev_samp_rating",
        "prev_city_stddev_samp_rating",
        "prev_seq_no",
        "next_seq_no",
        "seq_no_bucket",
        F.ntile(4).over(w7).alias("next_city_stddev_samp_rating_bucket"),
        F.round((F.col("event_seq") / F.sum("event_seq").over(w2)), 4).alias("event_seq_share"),
        F.rank().over(w8).alias("prev_seq_no_rank"),
        F.sum("cumulative_event_seq").over(w9).alias("cumulative_cumulative_event_seq"),
    )
)
