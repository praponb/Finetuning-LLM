from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("supplier_id").orderBy("cost", "id").rowsBetween(-6, Window.currentRow)

result = (
    df_products
    .withColumn("last7_min_cost", F.min("cost").over(w))
    .withColumn("last7_sum_cost", F.sum("cost").over(w))
    .withColumn("last7_max_cost", F.max("cost").over(w))
    .withColumn("last7_count_cost", F.count("cost").over(w))
)
