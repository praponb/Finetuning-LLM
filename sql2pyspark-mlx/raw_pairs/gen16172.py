from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.price").desc(), F.col("c.id").desc())
w2 = Window.partitionBy(F.date_trunc("YEAR", "launch_date"), "city")
w3 = Window.partitionBy("city", "country").orderBy(F.col("launch_date").desc(), "id")
w4 = Window.partitionBy("country").orderBy(F.col("launch_date").desc(), "id")
w_grp = Window.orderBy("rating", "id")
w6 = Window.orderBy(F.col("cost").desc(), "id")
w7 = Window.orderBy(F.col("rating").desc())
w8 = Window.orderBy("cost", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

eligible = (
    df_suppliers
    .alias("s")
    .join(df_products.alias("ch"), (F.col("ch.supplier_id") == F.col("s.id")) & (F.col("ch.price") > 740), "left_semi")
)

latest_match = (
    eligible
    .alias("s")
    .join(df_products.alias("c"), (F.col("c.supplier_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.launch_date"), F.col("c.cost"))
)

not_flagged = (
    latest_match
    .alias("s")
    .join(df_products.alias("x"), (F.col("x.supplier_id") == F.col("s.id")) & (F.col("x.launch_date") <= F.col("s.launch_date")), "left_semi")
)

enriched = (
    not_flagged
    .select(
        "id",
        "supplier_name",
        "rating",
        "launch_date",
        "cost",
        F.sum("rating").over(w2).alias("launch_date_period_sum_rating"),
        F.row_number().over(w3).alias("row_seq"),
        F.row_number().over(w4).alias("event_seq"),
    )
)

with_windows = (
    enriched
    .select(
        "*",
        F.ntile(5).over(w_grp).alias("rating_bucket"),
        F.ntile(5).over(w6).alias("cost_bucket"),
        F.rank().over(w7).alias("rating_rank"),
        F.ntile(5).over(w_grp).alias("rating_bucket_2"),
        F.first("launch_date_period_sum_rating", ignorenulls=True).over(w8).alias("first_known_launch_date_period_sum_rating"),
    )
)

result = (
    with_windows
    .groupBy(
        F.when(F.col("rating_rank") >= 9, "LARGE").otherwise("SMALL").alias("rating_rank_tier"),
        F.date_trunc("month", "launch_date").alias("activity_month"),
    )
    .agg(
        F.percentile_approx("rating", [0.25, 0.5, 0.75]).alias("rating_quartiles"),
        F.stddev_samp("rating").alias("rating_stddev"),
    )
)
