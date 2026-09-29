from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id", "product_id").orderBy(F.col("revenue_rounded").desc(), "id")

base = (
    df_store_sales
    .filter(F.col("units").between(31, 35))
    .select(
        "*",
        F.dayofmonth("sale_date").alias("sale_date_day"),
        F.round("revenue", 1).alias("revenue_rounded"),
    )
)

result = (
    base
    .withColumn("dedup_rank", F.row_number().over(w))
    .filter(F.col("dedup_rank") == 1)
    .drop("dedup_rank")
)
