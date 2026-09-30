from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_part = Window.partitionBy("customer_id").orderBy("amount", "id")
w2 = Window.partitionBy("status").orderBy("order_date", "id")

result = (
    df_orders
    .withColumn("next_amount", F.lead("amount").over(w_part))
    .withColumn("next_discount", F.lead("discount").over(w_part))
    .withColumn("prev_discount", F.lag("discount").over(w_part))
    .withColumn("seq_no", F.row_number().over(w_part))
    .withColumn("event_seq", F.row_number().over(w2))
)
