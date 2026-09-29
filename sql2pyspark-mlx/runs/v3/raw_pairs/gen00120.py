from pyspark.sql import functions as F

filtered_shipments = df_shipments.filter(F.col("cost") > 55)

agg = (
    filtered_shipments
    .groupBy("carrier", F.month("delivered_date").alias("delivered_month"))
    .agg(
        F.sum(F.when(F.col("carrier") == "UPS", F.col("cost")).otherwise(0)).alias("ups_cost"),
    )
)

result = (
    agg
    .select("carrier", "ups_cost")
    .orderBy("carrier")
)
