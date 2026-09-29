from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("segment").orderBy("age", "id")
w_ordered = Window.partitionBy("city").orderBy("age", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

result = (
    df_customers
    .withColumn("age_bucket", F.ntile(3).over(w))
    .withColumn("cumulative_age", F.sum("age").over(w_ordered))
    .withColumn("last_known_segment", F.last("segment", ignorenulls=True).over(w_ordered))
)
