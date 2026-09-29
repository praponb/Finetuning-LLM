from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(~((F.col("carrier") != "DHL") & F.col("order_id").isin([15, 26, 30]) & F.col("delivered_date").between("2024-01-11", "2024-10-01")))
    .groupBy("carrier")
    .agg(F.round(F.stddev("cost"), 2).alias("stddev_cost"))
)
