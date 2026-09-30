from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("cost").between(70, 75))
    .groupBy("carrier")
    .agg(F.count(F.when(F.col("status") == "DELIVERED", 1)).alias("delivered_count"))
    .filter(F.col("delivered_count") > 5)
    .orderBy("carrier")
)
