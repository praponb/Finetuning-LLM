from pyspark.sql import functions as F

src = (
    df_shipments
    .filter(~F.col("status").isin(["LOST", "DELIVERED"]) & (F.col("status") == "RETURNED"))
)

agg = (
    src
    .groupBy("carrier")
    .agg(
        F.sum(F.when(F.col("carrier") == "Kerry", 1).otherwise(0)).alias("kerry_count"),
    )
)

result = agg.orderBy(F.col("carrier").desc())
