from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy(F.col("created_at").cast("bigint")).rangeBetween(-3600, Window.currentRow)
w2 = Window.partitionBy("customer_id", "code").orderBy(F.col("credit_limit").desc_nulls_first())
w_hist = Window.partitionBy("code").orderBy(F.col("credit_limit").desc())
w4 = Window.partitionBy("code", "customer_id").orderBy(F.col("created_at").cast("bigint")).rangeBetween(-900, Window.currentRow)
w5 = Window.partitionBy("code").orderBy(F.col("credit_limit_dense_rank").desc_nulls_last())

exploded = (
    df_carts
    .select(
        "id",
        "customer_id",
        "created_at",
        F.explode_outer("coupon_codes").alias("code"),
    )
)

with_dims = (
    exploded
    .alias("src")
    .join(df_customers.alias("p"), F.col("src.customer_id") == F.col("p.id"), "left")
    .select("src.*", F.col("p.signup_date"), F.col("p.credit_limit"))
)

enriched = (
    with_dims
    .withColumn("credit_limit_count_1h", F.count("credit_limit").over(w))
    .withColumn("credit_limit_dense_rank", F.dense_rank().over(w2))
)

scored = (
    enriched
    .select(
        "customer_id",
        "code",
        F.dense_rank().over(w_hist).alias("credit_limit_dense_rank_2"),
        F.count("credit_limit_count_1h").over(w4).alias("credit_limit_count_1h_count_15m"),
        F.percent_rank().over(w5).alias("credit_limit_dense_rank_percent_rank"),
    )
)

result = (
    scored
    .groupBy(
        F.when(F.col("credit_limit_dense_rank_percent_rank") >= 0.89, "HIGH").otherwise("LOW").alias("credit_limit_dense_rank_percent_rank_tier"),
        F.when(F.col("credit_limit_dense_rank_2") >= 7, "HIGH").otherwise("LOW").alias("credit_limit_dense_rank_2_tier"),
    )
    .agg(
        F.size(F.collect_set("code")).alias("num_codes"),
        F.round(F.sum("credit_limit_dense_rank_2") / F.nullif(F.count(F.when(F.col("code") == "VIP", 1)), F.lit(0)), 2).alias("credit_limit_dense_rank_2_per_vip"),
        F.round(F.avg("credit_limit_dense_rank_2"), 2).alias("avg_credit_limit_dense_rank_2"),
    )
    .orderBy(F.col("credit_limit_dense_rank_2_per_vip").desc())
)
