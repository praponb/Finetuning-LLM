from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id", "carrier").orderBy("delivered_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_ordered = Window.partitionBy("carrier", "order_id").orderBy(F.col("cost").asc_nulls_last())

result = (
    df_shipments
    .withColumn("running_cost", F.sum("cost").over(w))
    .withColumn("cost_cume_dist", F.cume_dist().over(w_ordered))
)
