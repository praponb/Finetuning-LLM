from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id").orderBy(F.col("quantity").asc_nulls_last(), "id")

ranked = (
    df_inventory
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

result = (
    ranked
    .withColumn("week_start", F.date_trunc("WEEK", "last_restock"))
    .groupingSets([["store_id", "week_start"], ["store_id"]], "store_id", "week_start")
    .agg(
        F.bit_or("quantity").alias("quantity_bit_or"),
        F.stddev_samp("quantity").alias("quantity_stddev"),
        F.countDistinct("store_id").alias("distinct_store_ids"),
        F.grouping_id().alias("grouping_level"),
    )
)
