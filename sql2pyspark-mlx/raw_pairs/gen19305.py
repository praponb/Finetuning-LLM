from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy("delivered_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

result = (
    df_shipments
    .withColumn("running_max_cost", F.max("cost").over(w))
    .withColumn("running_avg_cost", F.avg("cost").over(w))
    .withColumn("running_count_cost", F.count("cost").over(w))
)
