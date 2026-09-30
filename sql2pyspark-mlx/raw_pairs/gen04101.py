from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.order_date").desc(), F.col("c.id").desc())

latest_match = (
    df_stores
    .alias("s")
    .join(df_orders.alias("c"), (F.col("c.store_id") == F.col("s.id")) & (F.col("c.order_date") <= F.col("s.opened_date")), "inner")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.store_name",
        "s.region",
        "s.opened_date",
        "s.sqft",
        F.col("c.discount"),
    )
)

result = (
    latest_match
    .groupBy(
        F.date_trunc("YEAR", "opened_date").alias("year_start"),
        F.when(F.col("sqft") >= 13200, "HIGH").otherwise("LOW").alias("sqft_tier"),
        "region",
    )
    .agg(
        F.percentile_approx("sqft", 0.5).alias("sqft_p50"),
        F.avg(F.when(F.col("region") == "West", F.col("discount"))).alias("west_avg_discount"),
        F.count_if(F.col("region") == "Central").alias("central_events"),
    )
)
