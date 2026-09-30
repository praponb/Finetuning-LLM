from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("brand").orderBy(F.col("cost").asc_nulls_first())
w2 = Window.partitionBy("brand").orderBy(F.col("launch_date").desc(), "id")
w3 = Window.orderBy("stock_qty", "id")

result = (
    df_products
    .withColumn("cost_dense_rank", F.dense_rank().over(w))
    .withColumn("row_seq", F.row_number().over(w2))
    .withColumn("next_cost", F.lead("cost", 1).over(w3))
)
