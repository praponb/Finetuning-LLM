from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("region", "city").orderBy(F.col("sqft").asc_nulls_last())

deduped = (
    df_stores
    .withColumn("rn", F.rank().over(w))
    .filter(F.col("rn") == 1)
)

result = (
    deduped
    .groupBy("region")
    .agg(
        F.count(F.when(F.col("region") == "South", 1)).alias("south_count"),
        F.bit_and("sqft").alias("sqft_bit_and"),
    )
    .filter(F.col("sqft_bit_and") > 1)
)
