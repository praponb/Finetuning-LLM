from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy(F.col("shipping_fee").asc_nulls_first(), "id")

base = (
    df_orders
    .filter((F.col("order_date") > F.lit("2023-06-09").cast("date")) & F.col("channel").isNotNull())
    .withColumn("channel_title", F.initcap(F.trim("channel")))
    .withColumn("channel_filled", F.coalesce("channel", F.lit("OTHER")))
)

result = (
    base
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
)
