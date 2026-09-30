from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.shipping_fee").desc(), F.col("c.id").desc())

latest_match = (
    df_stores
    .alias("s")
    .join(df_orders.alias("c"), (F.col("c.store_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.amount"))
)

result = (
    latest_match
    .groupBy("region")
    .agg(
        F.count("*").alias("row_count"),
        F.round(F.avg("sqft"), 2).alias("avg_sqft"),
        F.bit_and("sqft").alias("sqft_bit_and"),
        F.bit_and("sqft").alias("sqft_bit_and_2"),
    )
    .filter(F.col("sqft_bit_and_2") >= 1)
    .orderBy(F.col("sqft_bit_and_2").desc())
)
