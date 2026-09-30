from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("plan")
w2 = Window.orderBy(F.col("val").desc_nulls_last())

exploded = (
    df_price_tiers
    .select(
        "id",
        "plan",
        "effective_from",
        "currency",
        F.explode("discounts").alias("val"),
    )
)

base = (
    exploded
    .filter(F.col("currency") != "USD")
    .withColumn("currency_title", F.initcap(F.trim("currency")))
    .withColumn("days_since_effective_from", F.datediff(F.current_date(), "effective_from"))
)

cleaned = (
    base
    .filter(F.col("effective_from") > F.to_date(F.lit("2024-02-26")))
    .select(
        "id",
        "plan",
        "effective_from",
        "currency",
        "val",
        "currency_title",
        F.when(F.col("plan") == "basic", 1).otherwise(0).alias("is_basic"),
    )
)

metrics = (
    cleaned
    .select(
        "id",
        "effective_from",
        "currency",
        "currency_title",
        F.max("is_basic").over(w).alias("plan_max_is_basic"),
        F.min("val").over(w).alias("plan_min_val"),
        F.rank().over(w2).alias("val_rank"),
    )
)

filtered = (
    metrics
    .filter(F.col("plan_max_is_basic") < 31)
    .withColumn("effective_from_month_end", F.last_day("effective_from"))
)

result = (
    filtered
    .groupBy("currency")
    .agg(
        F.sum(F.when(F.col("currency") == "EUR", F.col("plan_min_val")).otherwise(0)).alias("eur_plan_min_val"),
        F.count(F.when(F.col("currency") == "THB", 1)).alias("thb_rows"),
        F.max("plan_max_is_basic").alias("max_plan_max_is_basic"),
    )
    .orderBy("eur_plan_min_val")
)
