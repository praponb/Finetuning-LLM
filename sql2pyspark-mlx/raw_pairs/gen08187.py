from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id")
w2 = Window.partitionBy("store_id").orderBy(F.col("units").asc_nulls_last())
w3 = Window.orderBy(F.col("days_since_sale_date").desc(), "id")
w_part = Window.partitionBy("store_id").orderBy("units", "id").rowsBetween(-5, Window.currentRow)

valid = (
    df_store_sales
    .filter(F.col("revenue") <= 3500)
    .select(
        "id",
        "store_id",
        "product_id",
        "units",
        F.datediff(F.current_date(), "sale_date").alias("days_since_sale_date"),
        F.when(F.col("units") >= 34, "High").when(F.col("units") >= 17, "Medium").otherwise("Low").alias("units_band"),
    )
)

result = (
    valid
    .withColumn("store_id_stddev_samp_units", F.stddev_samp("units").over(w))
    .withColumn("units_percent_rank", F.percent_rank().over(w2))
    .withColumn("row_seq", F.row_number().over(w3))
    .withColumn("days_since_sale_date_avg_5", F.avg("days_since_sale_date").over(w_part))
)
