from pyspark.sql import functions as F

result = (
    df_shipments
    .groupBy(F.year("delivered_date").alias("delivered_year"))
    .agg(
        F.round(F.avg("cost"), 2).alias("avg_cost"),
        F.countDistinct("cost").alias("unique_carriers"),
        F.sum(F.when(F.col("carrier") == "Flash", F.col("cost")).otherwise(0)).alias("flash_cost"),
    )
    .orderBy("flash_cost")
)
