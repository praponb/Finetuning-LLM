from pyspark.sql import functions as F

result = (
    df_products
    .groupBy(F.date_trunc("YEAR", "launch_date").alias("year_start"))
    .agg(
        F.percentile_approx("price", 0.9).alias("price_p90"),
        F.count("*").alias("cnt"),
        F.count("*").alias("event_count"),
    )
)
