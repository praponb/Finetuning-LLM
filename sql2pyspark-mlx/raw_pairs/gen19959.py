from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_part = Window.partitionBy("order_id").orderBy("discount", "id").rowsBetween(-2, Window.currentRow)

valid = (
    df_payments
    .filter((F.col("paid_at") >= "2023-04-01") & (F.col("paid_at") > F.to_timestamp(F.lit("2023-05-09 03:00:00"))))
    .select(
        "id",
        "order_id",
        "amount",
        "paid_at",
        F.initcap(F.trim("method")).alias("method_title"),
        F.quarter("paid_at").alias("paid_at_quarter"),
    )
)

joined = (
    valid
    .alias("e")
    .join(df_orders.alias("b"), F.col("e.order_id") == F.col("b.id"), "left")
    .filter(F.col("b.shipping_fee") >= 18)
    .select(
        "e.id",
        "e.order_id",
        "e.amount",
        "e.method_title",
        F.col("b.channel"),
        F.col("b.order_date"),
        F.col("b.discount"),
    )
)

scored = (
    joined
    .select(
        "id",
        "amount",
        "discount",
        F.count("discount").over(w_part).alias("last3_count_discount"),
        F.avg("discount").over(w_part).alias("last3_avg_discount"),
        F.min("discount").over(w_part).alias("last3_min_discount"),
    )
)

base = (
    scored
    .filter(F.col("last3_avg_discount") < 0.36)
    .withColumn("least_last3_count_discount_discount", F.least("last3_count_discount", "discount"))
    .withColumn("last3_avg_discount_plus_last3_count_discount", (F.col("last3_avg_discount") + F.col("last3_count_discount")))
)

result = (
    base
    .groupBy(
        F.when(F.col("least_last3_count_discount_discount") >= 69, "HIGH").otherwise("LOW").alias("least_last3_count_discount_discount_tier"),
    )
    .agg(
        F.percentile_approx("last3_count_discount", [0.25, 0.5, 0.75]).alias("last3_count_discount_quartiles"),
        F.percentile_approx("last3_avg_discount_plus_last3_count_discount", [0.25, 0.5, 0.75]).alias("last3_avg_discount_plus_last3_count_discount_quartiles"),
        F.percentile_approx("least_last3_count_discount_discount", [0.25, 0.5, 0.75]).alias("least_last3_count_discount_discount_quartiles"),
    )
    .orderBy(F.col("least_last3_count_discount_discount_tier").desc_nulls_first())
)
