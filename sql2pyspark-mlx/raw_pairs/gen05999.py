from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status", "store_id")
w2 = Window.partitionBy("store_id").orderBy("shipping_fee", "id").rowsBetween(-2, -1)
w3 = Window.partitionBy("channel").orderBy("amount", "id")
w4 = Window.partitionBy("customer_id").orderBy(F.col("discount").asc_nulls_last())
w5 = Window.partitionBy("channel")

scored = (
    df_orders
    .filter(F.col("status").isin(["CANCELLED", "SHIPPED"]))
    .withColumn("status_avg_amount", F.avg("amount").over(w))
    .withColumn("discount_avg_2_prior", F.avg("discount").over(w2))
    .withColumn("prev_discount", F.lag("discount", 1).over(w3))
    .withColumn("discount_cume_dist", F.cume_dist().over(w4))
    .withColumn("corr_amount_discount", F.corr("amount", "discount").over(w5))
)

result = (
    scored
    .groupBy(
        F.when(F.col("discount_avg_2_prior") >= 0, "HIGH").otherwise("LOW").alias("discount_avg_2_prior_tier"),
    )
    .agg(
        F.percentile_approx("discount_avg_2_prior", 0.5).alias("discount_avg_2_prior_p50"),
        F.round(F.sum("discount_cume_dist") / F.nullif(F.count(F.when(F.col("channel") == "web", 1)), F.lit(0)), 2).alias("discount_cume_dist_per_web"),
    )
    .orderBy(F.col("discount_cume_dist_per_web").desc_nulls_last())
)
