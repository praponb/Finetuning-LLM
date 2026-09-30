from pyspark.sql import functions as F

result = (
    df_products
    .groupBy(F.date_trunc("week", "launch_date").alias("week_start"))
    .agg(
        F.percentile_approx("cost", 0.9).alias("cost_p90"),
        F.count_if(F.col("brand") == "Initech").alias("initech_events"),
    )
    .filter(F.col("initech_events") < 1000)
    .orderBy(F.col("initech_events").desc())
)
