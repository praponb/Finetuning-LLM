from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.month("created_at"), "code").orderBy(F.col("created_at").cast("bigint")).rangeBetween(-1800, Window.currentRow)
w2 = Window.partitionBy("code").orderBy(F.col("code_length").desc())
w3 = Window.partitionBy("code")
w4 = Window.partitionBy("code").orderBy(F.col("code_length").asc_nulls_last())
w5 = Window.orderBy(F.col("created_at").cast("bigint")).rangeBetween(-3600, Window.currentRow)

exploded = df_carts.select("id", "created_at", F.explode_outer("coupon_codes").alias("code"))

base = (
    exploded
    .filter(F.col("code").isNotNull() & (F.col("code") == "SPRING10"))
    .select("*", F.trim("code").alias("clean_code"))
)

base_carts = (
    base
    .filter(F.col("created_at") > F.lit("2025-03-01 12:00:00").cast("timestamp"))
    .select(
        "*",
        F.when(F.col("code").isin(["SPRING10", "VIP"]), "Tier 1").otherwise("Other").alias("code_group"),
        F.length("code").alias("code_length"),
        F.coalesce("code", F.lit("OTHER")).alias("code_clean"),
    )
)

scored = (
    base_carts
    .filter(F.col("code").isin(["WELCOME5", "VIP"]))
    .withColumn("code_length_avg_30m", F.avg("code_length").over(w))
    .withColumn("code_length_dense_rank", F.dense_rank().over(w2))
    .withColumn("code_sum_code_length", F.sum("code_length").over(w3))
    .withColumn("code_length_cume_dist", F.cume_dist().over(w4))
    .withColumn("code_length_max_1h", F.max("code_length").over(w5))
)

result = (
    scored
    .select(
        "id",
        "created_at",
        "code",
        "clean_code",
        "code_group",
        "code_length",
        "code_clean",
        "code_length_avg_30m",
        "code_length_dense_rank",
    )
    .orderBy(F.col("code_length_dense_rank").desc_nulls_last())
)
