from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city").orderBy("sqft", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("city").orderBy("sqft", "id").rowsBetween(-2, Window.currentRow)
w3 = Window.partitionBy("region").orderBy("sqft", "id")

enriched = (
    df_stores
    .withColumn("cumulative_sqft", F.sum("sqft").over(w))
    .withColumn("sqft_avg_2", F.avg("sqft").over(w2))
    .withColumn("event_seq", F.row_number().over(w3))
)

result = (
    enriched
    .withColumn("year_start", F.date_trunc("YEAR", "opened_date"))
    .withColumn("week_start", F.date_trunc("WEEK", "opened_date"))
    .groupingSets(
        [["year_start", "week_start"], ["year_start"], []],
        "year_start",
        "week_start",
    )
    .agg(
        F.round(F.avg("sqft_avg_2"), 2).alias("avg_sqft_avg_2"),
        F.percentile_approx("event_seq", [0.25, 0.5, 0.75]).alias("event_seq_quartiles"),
        F.percentile_approx("sqft", 0.95).alias("sqft_p95"),
    )
    .orderBy(F.col("year_start").asc_nulls_last())
)
