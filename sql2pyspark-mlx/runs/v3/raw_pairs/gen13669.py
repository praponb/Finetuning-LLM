from pyspark.sql import functions as F

result = (
    df_shipments
    .groupBy(F.year("delivered_date").alias("delivered_year"), "carrier")
    .agg(
        F.countDistinct("cost").alias("unique_orders"),
        F.round(F.stddev("cost"), 2).alias("stddev_cost"),
        F.max("cost").alias("max_cost"),
    )
    .orderBy(F.col("stddev_cost").desc(), "delivered_year", "carrier")
    .limit(20)
)
