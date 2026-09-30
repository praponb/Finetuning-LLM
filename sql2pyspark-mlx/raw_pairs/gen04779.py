from pyspark.sql import functions as F

result = (
    df_shipments
    .groupBy("carrier")
    .agg(
        (F.max("cost") - F.min("cost")).alias("cost_range"),
        F.max("cost").alias("max_cost"),
    )
    .orderBy("cost_range")
)
