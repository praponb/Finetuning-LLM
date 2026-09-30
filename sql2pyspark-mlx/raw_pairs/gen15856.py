from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_grp = Window.partitionBy("code")
w2 = Window.partitionBy("code").orderBy(F.col("code_max_pos").desc())
w3 = Window.partitionBy("code").orderBy(F.col("pos").asc_nulls_last())
w4 = Window.partitionBy("code").orderBy("code_max_pos")
w5 = Window.orderBy(F.col("code_max_pos").asc_nulls_last())

flattened = (
    df_carts
    .select("*", F.posexplode_outer("items").alias("pos", "li_item"))
    .select(
        "id",
        "customer_id",
        "coupon_codes",
        "pos",
        F.col("li_item.product_id"),
        F.col("li_item.qty"),
    )
)

flattened_carts = flattened.select("pos", F.explode_outer("coupon_codes").alias("code"))

cleaned = (
    flattened_carts
    .filter(F.col("pos").between(1, 3))
    .withColumn("is_vip", F.when(F.col("code") == "VIP", 1).otherwise(0))
    .withColumn("code_lower", F.lower("code"))
)

enriched = cleaned.select("*", F.max("pos").over(w_grp).alias("code_max_pos"))

base = (
    enriched
    .filter(F.col("code").isNotNull() & (F.col("code_max_pos") > 3))
    .withColumn("code_upper", F.upper("code"))
    .withColumn("code_clean", F.coalesce("code", F.lit("DIRECT")))
)

scored = (
    base
    .select(
        "pos",
        "code",
        "code_lower",
        "code_max_pos",
        "code_clean",
        F.min("pos").over(w_grp).alias("code_min_pos"),
        F.percent_rank().over(w2).alias("code_max_pos_percent_rank"),
        F.percent_rank().over(w3).alias("pos_percent_rank"),
        F.cume_dist().over(w4).alias("code_max_pos_cume_dist"),
        F.rank().over(w5).alias("code_max_pos_rank"),
    )
)

result = (
    scored
    .select(
        "pos",
        "code",
        "code_lower",
        "code_max_pos",
        "code_clean",
        "code_max_pos_percent_rank",
        "code_max_pos_cume_dist",
        F.when(F.col("code").rlike("^(VIP|WELCOME5)$"), "GROUP_B").otherwise("OTHER_ZONE").alias("code_zone"),
    )
    .orderBy(F.col("code_max_pos_cume_dist").desc())
)
