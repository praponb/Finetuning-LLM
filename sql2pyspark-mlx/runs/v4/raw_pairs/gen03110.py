from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy(F.col("discount").desc(), "id")

result = (
    df_orders
    .withColumn("rnk", F.row_number().over(w))
    .filter(F.col("rnk") <= 5)
    .select("id", "customer_id", "discount", "channel")
)
