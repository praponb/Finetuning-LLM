from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_running = Window.orderBy(F.col("created_at").cast("bigint")).rangeBetween(-3600, Window.currentRow)
w2 = Window.partitionBy("country", "customer_id").orderBy(F.col("created_at").cast("bigint")).rangeBetween(-7200, Window.currentRow)
w3 = Window.partitionBy("country").orderBy(F.col("created_at").cast("bigint")).rangeBetween(-3600, Window.currentRow)
w4 = Window.partitionBy(F.year("created_at"))
w5 = Window.partitionBy(F.year("created_at"), "customer_id").orderBy("created_at_weekofyear")
w_recent = Window.orderBy(F.col("created_at").cast("bigint")).rangeBetween(-1800, Window.currentRow)

exploded = (
    df_carts
    .select(
        "id",
        "customer_id",
        "created_at",
        F.explode_outer("coupon_codes").alias("element"),
    )
)

enriched = (
    exploded
    .alias("e")
    .join(df_customers.alias("b"), F.col("e.customer_id") == F.col("b.id"), "left")
    .select("e.customer_id", "e.created_at", F.col("b.country"))
)

valid = (
    enriched
    .filter((F.col("created_at") >= F.lit("2025-03-02 03:00:00").cast("timestamp")) & F.col("customer_id").isNotNull())
    .select("*", F.weekofyear("created_at").alias("created_at_weekofyear"))
)

enriched_carts = (
    valid
    .select(
        "customer_id",
        "created_at",
        F.sum("created_at_weekofyear").over(w_running).alias("created_at_weekofyear_sum_1h"),
        F.sum("created_at_weekofyear").over(w2).alias("created_at_weekofyear_sum_2h"),
        F.count("created_at_weekofyear").over(w3).alias("created_at_weekofyear_count_1h"),
        F.min("created_at_weekofyear").over(w4).alias("created_at_year_min_created_at_weekofyear"),
        F.rank().over(w5).alias("created_at_weekofyear_rank"),
    )
)

with_windows = (
    enriched_carts
    .withColumn("created_at_weekofyear_count_1h_max_30m", F.max("created_at_weekofyear_count_1h").over(w_recent))
)

result = (
    with_windows
    .select(
        "created_at",
        "created_at_weekofyear_sum_1h",
        "created_at_weekofyear_count_1h",
        "created_at_year_min_created_at_weekofyear",
        "created_at_weekofyear_rank",
    )
    .orderBy(F.col("created_at_weekofyear_rank").asc_nulls_last())
)
