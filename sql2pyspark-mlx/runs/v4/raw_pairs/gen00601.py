from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_hist = Window.partitionBy("customer_id").orderBy("amount", "id")
w_grp = Window.partitionBy("status").orderBy("order_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_recent = Window.partitionBy("store_id").orderBy("discount", "id")

result = (
    df_orders
    .withColumn("amount_bucket", F.ntile(5).over(w_hist))
    .withColumn("running_amount", F.sum("amount").over(w_grp))
    .withColumn("discount_bucket", F.ntile(4).over(w_recent))
)
