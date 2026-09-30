from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("value_2").orderBy(F.col("value_2_length").desc_nulls_first())
w2 = Window.partitionBy("value_2").orderBy(F.col("value_2_length").desc())

exploded = (
    df_carts
    .select(
        "id",
        "customer_id",
        "coupon_codes",
        F.explode_outer("attributes").alias("k", "value"),
    )
)

exploded_carts = exploded.select("id", "value", F.explode_outer("coupon_codes").alias("value_2"))

valid = (
    exploded_carts
    .filter(F.col("value_2") == "SPRING10")
    .withColumn("value_2_length", F.length("value_2"))
)

scored = (
    valid
    .withColumn("value_2_length_dense_rank", F.dense_rank().over(w))
    .withColumn("value_2_length_cume_dist", F.cume_dist().over(w2))
)

valid_carts = (
    scored
    .filter(F.col("value_2").isNotNull())
    .select(
        "*",
        (F.col("value_2_length_cume_dist") * F.col("value_2_length_dense_rank")).alias("value_2_length_cume_dist_times_value_2_length_dense_rank"),
        F.coalesce("value_2_length", F.lit(0)).alias("value_2_length_filled"),
        F.coalesce("value_2", F.lit("NONE")).alias("resolved_value_2"),
    )
)

result = (
    valid_carts
    .select(
        "id",
        "value",
        "value_2",
        "value_2_length",
        "value_2_length_dense_rank",
        "value_2_length_cume_dist",
        "value_2_length_cume_dist_times_value_2_length_dense_rank",
        "value_2_length_filled",
        F.when(F.col("value_2").rlike("^(WELCOME5|SPRING10)$"), "EXTENDED").otherwise("OTHER_ZONE").alias("value_2_zone"),
    )
    .orderBy(F.col("value_2_zone").desc_nulls_last(), "value_2_length_dense_rank")
)
