from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status", "store_id").orderBy(F.col("discount").desc(), "id")

deduped = (
    df_orders
    .withColumn("rank_in_group", F.row_number().over(w))
    .filter(F.col("rank_in_group") == 1)
)

result = (
    deduped
    .groupBy(F.date_trunc("WEEK", "order_date").alias("week_start"))
    .agg(
        F.sort_array(F.collect_set("status")).alias("status_list"),
        F.round(F.sum("amount") / F.nullif(F.count(F.when(F.col("channel") == "partner", 1)), F.lit(0)), 2).alias("amount_per_partner"),
        F.percentile_approx("discount", 0.95).alias("discount_p95"),
        F.corr("amount", "discount").alias("corr_amount_discount"),
        F.countDistinct("store_id").alias("distinct_store_ids"),
    )
    .orderBy(F.col("discount_p95").desc_nulls_last())
)
