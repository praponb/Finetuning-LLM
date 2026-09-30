from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.year("effective_from"), "currency")
w2 = Window.partitionBy(F.date_trunc("WEEK", "effective_from"), "plan").orderBy(F.col("element").desc_nulls_last())
w3 = Window.partitionBy("currency")
w4 = Window.partitionBy("plan", "currency")
w_running = Window.orderBy("effective_from_year_max_element")
w6 = Window.partitionBy("currency", "plan").orderBy(F.col("effective_from_year_max_element").desc())

lines = (
    df_price_tiers
    .select(
        "id",
        "plan",
        "effective_from",
        "currency",
        F.explode_outer("discounts").alias("element"),
    )
)

scored = (
    lines
    .select(
        "*",
        F.max("element").over(w).alias("effective_from_year_max_element"),
        F.dense_rank().over(w2).alias("element_dense_rank"),
        F.max("element").over(w3).alias("currency_max_element"),
        F.sum("element").over(w4).alias("plan_sum_element"),
    )
)

base = (
    scored
    .filter((F.col("effective_from") >= F.lit("2024-04-14").cast("date")) & (F.col("effective_from") <= "2023-04-01"))
    .select(
        "id",
        "plan",
        "effective_from",
        "currency",
        "element",
        "effective_from_year_max_element",
        "element_dense_rank",
        "currency_max_element",
        "plan_sum_element",
        F.length("plan").alias("plan_length"),
    )
)

kept_keys = (
    base
    .select("currency", "id")
    .intersectAll(
        df_price_tiers
        .filter(F.col("effective_from") > "2025-08-26")
        .select("currency", "id")
    )
)

kept_rows = (
    base
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.currency") == F.col("k.currency")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

filtered = (
    kept_rows
    .filter(F.col("effective_from") >= F.lit("2024-05-23").cast("date"))
    .select(
        "id",
        "plan",
        "effective_from",
        "currency",
        "effective_from_year_max_element",
        "element_dense_rank",
        "plan_sum_element",
        F.concat("currency", F.lit(", "), "plan").alias("currency_plan"),
    )
)

result = (
    filtered
    .select(
        "id",
        "plan",
        "effective_from",
        "element_dense_rank",
        F.rank().over(w_running).alias("effective_from_year_max_element_rank"),
        F.dense_rank().over(w6).alias("effective_from_year_max_element_dense_rank"),
    )
)
