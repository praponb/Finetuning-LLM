from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id").orderBy(F.col("sale_date").asc_nulls_last(), "id")

ranked = (
    df_store_sales
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

result = (
    ranked
    .withColumn("revenue_tier", F.when(F.col("revenue") >= 3400, "PREMIUM").otherwise("STANDARD"))
    .groupingSets(
        [["revenue_tier", "store_id"], ["revenue_tier"], []],
        "revenue_tier",
        "store_id",
    )
    .agg(
        F.stddev_samp("revenue").alias("revenue_stddev"),
        F.bit_and("units").alias("units_bit_and"),
        F.percentile_approx("revenue", 0.5).alias("revenue_p50"),
        F.min("revenue").alias("min_revenue"),
        F.bit_and("units").alias("units_bit_and_2"),
        F.grouping("revenue_tier").alias("grouping_level"),
    )
    .orderBy(F.col("units_bit_and_2").desc())
)
