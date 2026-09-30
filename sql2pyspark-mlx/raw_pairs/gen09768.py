from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy("cost", "id")
w_recent = Window.orderBy("cost", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

result = (
    df_shipments
    .withColumn("prev_cost", F.lag("cost", 2).over(w))
    .withColumn("running_cost", F.sum("cost").over(w_recent))
)
