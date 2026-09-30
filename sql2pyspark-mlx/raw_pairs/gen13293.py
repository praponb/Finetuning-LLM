from pyspark.sql import functions as F

result = (
    df_shipments
    .groupBy(
        F.quarter("delivered_date").alias("delivered_quarter"),
        F.date_trunc("MONTH", "shipped_date").alias("shipped_month"),
    )
    .agg(F.round(F.stddev("cost"), 2).alias("stddev_cost"))
    .orderBy(F.col("shipped_month").desc())
)
