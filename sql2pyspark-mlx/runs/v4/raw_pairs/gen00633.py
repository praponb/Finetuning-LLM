from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id").orderBy("units", "id")
w2 = Window.partitionBy("store_id").orderBy("revenue", "id").rowsBetween(-3, Window.currentRow)
w3 = Window.orderBy("revenue", "id")
w_part = Window.partitionBy("product_id").orderBy(F.col("units").desc_nulls_first())

metrics = (
    df_store_sales
    .withColumn("prev_units", F.lag("units", 1).over(w))
    .withColumn("revenue_avg_3", F.avg("revenue").over(w2))
    .withColumn("prev_units_2", F.lag("units", 1).over(w3))
    .withColumn("units_rank", F.rank().over(w_part))
)

result = (
    metrics
    .select(
        "id",
        "store_id",
        "product_id",
        "revenue",
        "prev_units",
        "revenue_avg_3",
        "prev_units_2",
        "units_rank",
    )
)
