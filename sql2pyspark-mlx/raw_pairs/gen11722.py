from pyspark.sql import functions as F

result = (
    df_shipments
    .filter((F.col("status") != "LOST") & F.col("carrier").isNull() & F.col("order_id").isNotNull())
    .groupBy("carrier")
    .agg(
        F.max("cost").alias("max_cost"),
        F.sum("cost").alias("total_cost"),
    )
    .orderBy("max_cost")
)
