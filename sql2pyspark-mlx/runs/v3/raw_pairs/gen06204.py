from pyspark.sql import functions as F

result = (
    df_shipments
    .groupBy("carrier")
    .agg(
        (F.max("cost") - F.min("cost")).alias("cost_range"),
        F.count("*").alias("cnt"),
    )
    .filter(F.col("cnt") < 2)
    .orderBy("cnt")
)
