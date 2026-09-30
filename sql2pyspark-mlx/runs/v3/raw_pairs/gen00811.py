from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("status").isin(["RETURNED", "DELIVERED"]) & (F.col("delivered_date") > "2023-07-01"))
    .groupBy("status")
    .agg(
        F.max("cost").alias("max_cost"),
        F.countDistinct("cost").alias("unique_statuss"),
    )
    .filter(F.col("max_cost") < 30)
    .orderBy("status")
)
