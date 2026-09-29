from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_running = Window.partitionBy("element").orderBy(F.col("created_at_hour").desc_nulls_first())
w2 = Window.partitionBy("element")
w3 = Window.orderBy(F.col("element_max_created_at_hour").desc())
w4 = Window.orderBy("years_since_created_at")
w5 = Window.partitionBy(F.month("created_at")).orderBy(F.col("created_at").cast("bigint")).rangeBetween(-3600, Window.currentRow)
w6 = Window.partitionBy(F.year("created_at")).orderBy(F.col("created_at").cast("bigint")).rangeBetween(-900, Window.currentRow)

exploded = (
    df_carts
    .select(
        "id",
        "created_at",
        "coupon_codes",
        F.explode_outer("attributes").alias("key", "attr_value"),
    )
)

flattened = (
    exploded
    .select(
        "id",
        "created_at",
        "attr_value",
        F.explode("coupon_codes").alias("element"),
    )
)

cleaned = (
    flattened
    .filter(F.col("element") == "BUNDLE")
    .select("*", F.date_trunc("YEAR", "created_at").alias("created_at_period"))
)

filtered = (
    cleaned
    .filter((F.col("created_at") > F.lit("2025-03-02 03:00:00").cast("timestamp")) & F.col("element").isNotNull())
    .select(
        "id",
        "created_at",
        "attr_value",
        "element",
        "created_at_period",
        F.hour("created_at").alias("created_at_hour"),
    )
)

scored = (
    filtered
    .filter(F.col("element") == "BUNDLE")
    .select(
        "id",
        "created_at",
        "attr_value",
        F.dense_rank().over(w_running).alias("created_at_hour_dense_rank"),
        F.max("created_at_hour").over(w2).alias("element_max_created_at_hour"),
    )
)

base = (
    scored
    .filter(F.to_date("created_at") >= "2024-04-01")
    .withColumn("years_since_created_at", (F.datediff(F.current_date(), "created_at") / 365.25))
)

result = (
    base
    .select(
        "id",
        "created_at",
        F.cume_dist().over(w3).alias("element_max_created_at_hour_cume_dist"),
        F.dense_rank().over(w4).alias("years_since_created_at_dense_rank"),
        F.max("created_at_hour_dense_rank").over(w5).alias("created_at_hour_dense_rank_max_1h"),
        F.max("created_at_hour_dense_rank").over(w6).alias("created_at_hour_dense_rank_max_15m"),
    )
)
