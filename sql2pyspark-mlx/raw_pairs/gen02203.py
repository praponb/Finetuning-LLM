from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city").orderBy("rating", "id")
w2 = Window.orderBy("rating", "id")
w3 = Window.orderBy("rating", "id").rowsBetween(-2, Window.currentRow)
w4 = Window.orderBy("rating", "id").rowsBetween(-3, -1)
w5 = Window.orderBy("rating", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.orderBy("rating_avg_3_prior", "id")
w7 = Window.orderBy("cumulative_rating", "id")

base = (
    df_suppliers
    .filter(F.col("city").isNull())
    .select("*", F.substring("city", 1, 2).alias("city_prefix"))
)

deduped = (
    base
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") == 1)
    .select("id", "supplier_name", "rating")
)

enriched = (
    deduped
    .filter(F.col("rating").between(2.1, 3.4))
    .select(
        "id",
        "supplier_name",
        F.row_number().over(w2).alias("seq_no"),
        F.avg("rating").over(w3).alias("rating_avg_2"),
        F.avg("rating").over(w4).alias("rating_avg_3_prior"),
        F.sum("rating").over(w5).alias("cumulative_rating"),
    )
)

metrics = (
    enriched
    .withColumn("event_seq", F.row_number().over(w6))
    .withColumn("row_seq", F.row_number().over(w7))
)

not_flagged = (
    metrics
    .alias("s")
    .join(df_products.alias("ch"), (F.col("ch.supplier_id") == F.col("s.id")) & (F.col("ch.stock_qty") < 304), "left_anti")
)

grouped = (
    not_flagged
    .groupBy("seq_no")
    .agg(
        F.avg("event_seq").alias("avg_event_seq"),
        F.round(F.avg("cumulative_rating"), 2).alias("avg_cumulative_rating"),
        F.count("*").alias("event_count"),
        F.count("*").alias("row_count"),
        F.percentile_approx("cumulative_rating", [0.25, 0.5, 0.75]).alias("cumulative_rating_quartiles"),
    )
)

result = (
    grouped
    .select(
        "seq_no",
        "avg_event_seq",
        "avg_cumulative_rating",
        "event_count",
        "row_count",
        "cumulative_rating_quartiles",
    )
    .orderBy(F.col("event_count").desc())
)
