from pyspark.sql import functions as F

recent_shipments = (
    df_shipments
    .filter((F.col("shipped_date") > "2023-08-21") & F.month("delivered_date").isin([4, 6, 10]))
)

delivered_year_summary = (
    recent_shipments
    .groupBy(F.year("delivered_date").alias("delivered_year"))
    .agg(
        F.round(F.stddev("cost"), 2).alias("stddev_cost"),
        F.min("cost").alias("min_cost"),
    )
)

result = delivered_year_summary.select("delivered_year", "stddev_cost")
