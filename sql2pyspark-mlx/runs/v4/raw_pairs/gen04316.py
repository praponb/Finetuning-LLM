from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("credit_limit")
w2 = Window.partitionBy("customer_id").orderBy(F.col("is_vip").desc())
w3 = Window.partitionBy("customer_id").orderBy(F.col("is_vip").desc_nulls_last())
w4 = Window.partitionBy("customer_id")
w5 = Window.orderBy(F.col("is_vip_dense_rank").desc_nulls_last())
balance_by_customer_id = df_accounts.groupBy("customer_id").agg(F.avg("balance").alias("avg_account_balance"))

exploded = (
    df_carts
    .withColumn("elem", F.explode("items"))
    .select(
        "id",
        "customer_id",
        "coupon_codes",
        F.col("elem.qty"),
        F.col("elem.price"),
        F.col("elem.promo"),
    )
)

exploded_carts = (
    exploded
    .select("id", "customer_id", F.explode_outer("coupon_codes").alias("element"))
)

joined = (
    exploded_carts
    .alias("s")
    .join(df_customers.alias("c"), F.col("s.customer_id") == F.col("c.id"), "inner")
    .select("s.*", F.col("c.credit_limit"))
)

base = (
    joined
    .filter(F.col("element").isNotNull())
    .select(
        "*",
        F.when(F.col("element") == "VIP", 1).otherwise(0).alias("is_vip"),
        F.coalesce("element", F.lit("DIRECT")).alias("element_filled"),
    )
)

with_windows = (
    base
    .select(
        "*",
        F.rank().over(w).alias("credit_limit_rank"),
        F.dense_rank().over(w2).alias("is_vip_dense_rank"),
        F.dense_rank().over(w3).alias("is_vip_dense_rank_2"),
    )
)

with_windows_carts = (
    with_windows
    .withColumn("is_vip_dense_rank_share", F.round((F.col("is_vip_dense_rank") / F.sum("is_vip_dense_rank").over(w4)), 4))
    .withColumn("customer_id_max_credit_limit_rank", F.max("credit_limit_rank").over(w4))
    .withColumn("is_vip_dense_rank_percent_rank", F.percent_rank().over(w5))
    .withColumn("credit_limit_share", F.round((F.col("credit_limit") / F.sum("credit_limit").over(w4)), 4))
)

result = (
    with_windows_carts
    .join(balance_by_customer_id, on="customer_id", how="left")
    .select(
        "id",
        "customer_id",
        "element",
        "is_vip",
        "element_filled",
        "credit_limit_rank",
        "is_vip_dense_rank",
        "is_vip_dense_rank_2",
        "is_vip_dense_rank_percent_rank",
        F.col("avg_account_balance"),
    )
    .orderBy(F.col("is_vip_dense_rank_2").desc_nulls_first())
)
