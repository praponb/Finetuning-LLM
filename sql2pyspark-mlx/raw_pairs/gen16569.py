from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy(F.col("amount").desc_nulls_first(), "id")

deduped = (
    df_orders
    .withColumn("rank_in_group", F.row_number().over(w))
    .filter(F.col("rank_in_group") == 1)
    .drop("rank_in_group")
)

result = (
    deduped
    .select("id", "customer_id", "order_date", "status", "channel", "discount")
    .orderBy(F.col("discount").desc(), "channel")
)
