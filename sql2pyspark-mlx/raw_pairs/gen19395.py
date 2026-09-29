from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy(F.col("paid_at").asc_nulls_last(), "id")

deduped = (
    df_payments
    .withColumn("rank_in_group", F.row_number().over(w))
    .filter(F.col("rank_in_group") == 1)
    .select("id", "order_id", "method")
)

result = (
    deduped
    .groupBy("order_id")
    .agg(
        F.count(F.when(F.col("method") == "card", 1)).alias("card_rows"),
        F.sort_array(F.collect_set("method")).alias("method_list"),
        F.count(F.when(F.col("method") == "paypal", 1)).alias("paypal_rows"),
    )
    .filter(F.col("card_rows") > 1)
)
