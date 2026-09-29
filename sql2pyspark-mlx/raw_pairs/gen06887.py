from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy(F.col("sale_date").asc_nulls_last(), "id")

deduped = (
    df_store_sales
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") == 1)
    .select("id", "store_id", "product_id", "units")
)

result = (
    deduped
    .withColumn("units_tier", F.when(F.col("units") >= 40, "HIGH").otherwise("LOW"))
    .rollup("store_id", "units_tier")
    .agg(
        F.countDistinct("store_id").alias("distinct_store_ids"),
        F.bit_or("units").alias("units_bit_or"),
        F.sum("units").alias("total_units"),
        F.max("units").alias("max_units"),
        F.stddev_samp("units").alias("units_stddev"),
        F.grouping_id().alias("grouping_level"),
    )
    .orderBy(F.col("units_tier").desc())
)
