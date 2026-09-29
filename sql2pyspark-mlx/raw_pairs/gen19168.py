from pyspark.sql import functions as F

result = (
    df_shipments
    .groupBy("carrier")
    .agg(
        F.sort_array(F.collect_set("carrier")).alias("carrier_list"),
        F.max("cost").alias("max_cost"),
        F.sum(F.when(F.col("status") == "DELIVERED", F.col("cost")).otherwise(0)).alias("delivered_cost"),
    )
    .orderBy(F.col("max_cost").desc())
)
