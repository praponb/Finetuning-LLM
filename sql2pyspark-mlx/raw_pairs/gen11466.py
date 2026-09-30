from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("amount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_grp = Window.partitionBy("channel", "store_id").orderBy("amount", "id")

result = (
    df_orders
    .withColumn("first_known_amount", F.first("amount", ignorenulls=True).over(w))
    .withColumn("amount_bucket", F.ntile(4).over(w_grp))
)
