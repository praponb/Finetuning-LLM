from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy(F.col("paid_at").asc_nulls_first(), "id")

deduped = (
    df_payments
    .withColumn("dedup_rank", F.row_number().over(w))
    .filter(F.col("dedup_rank") == 1)
)

result = (
    deduped
    .cube(F.date_trunc("YEAR", "paid_at").alias("year_start"))
    .agg(
        F.sort_array(F.collect_set("method")).alias("method_list"),
        F.sort_array(F.collect_set("method")).alias("method_list_2"),
        F.sum(F.when(F.col("status") == "SUCCESS", F.col("amount"))).alias("success_total_amount"),
    )
    .orderBy(F.col("year_start").desc())
)
