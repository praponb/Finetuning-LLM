from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("supplier_id")
w2 = Window.partitionBy("brand", "supplier_id").orderBy(F.col("cost").desc_nulls_last())
w3 = Window.partitionBy("brand").orderBy(F.col("cost").desc())
w4 = Window.partitionBy("brand").orderBy("launch_date", "id").rowsBetween(-2, Window.currentRow)
w_recent = Window.partitionBy("brand").orderBy("last3_sum_cost_share", "id").rowsBetween(-2, Window.currentRow)
w6 = Window.partitionBy(F.year("launch_date")).orderBy(F.col("last3_max_cost_share").desc(), "id")

valid = (
    df_products
    .filter((F.col("launch_date") >= F.lit("2023-05-20").cast("date")) & (~F.col("brand").eqNullSafe(F.lit("Acme"))))
    .select(
        "id",
        "brand",
        "cost",
        "launch_date",
        "tags",
        "supplier_id",
        F.upper("brand").alias("brand_upper"),
        F.last_day("launch_date").alias("launch_date_month_end"),
        F.coalesce("brand", F.lit("N/A")).alias("resolved_brand"),
    )
)

with_windows = (
    valid
    .filter(F.col("launch_date") > "2023-04-01")
    .select(
        "id",
        "brand",
        "cost",
        "launch_date",
        "tags",
        "launch_date_month_end",
        "resolved_brand",
        F.round((F.col("cost") / F.sum("cost").over(w)), 4).alias("cost_share"),
        F.rank().over(w2).alias("cost_rank"),
    )
)

ranked = (
    with_windows
    .withColumn("latest_rank", F.rank().over(w3))
    .filter(F.col("latest_rank") == 1)
)

scored = (
    ranked
    .select(
        "*",
        F.sum("cost_share").over(w4).alias("last3_sum_cost_share"),
        F.min("cost_share").over(w4).alias("last3_min_cost_share"),
        F.max("cost_share").over(w4).alias("last3_max_cost_share"),
    )
)

scored_products = (
    scored
    .withColumn("last3_min_last3_max_cost_share", F.min("last3_max_cost_share").over(w_recent))
    .withColumn("last3_sum_last3_max_cost_share", F.sum("last3_max_cost_share").over(w_recent))
    .withColumn("last3_max_last3_max_cost_share", F.max("last3_max_cost_share").over(w_recent))
    .withColumn("last3_count_last3_max_cost_share", F.count("last3_max_cost_share").over(w_recent))
    .withColumn("event_seq", F.row_number().over(w6))
)

result = (
    scored_products
    .select(
        "id",
        "brand",
        "cost",
        "launch_date",
        "tags",
        "resolved_brand",
        "cost_share",
        "cost_rank",
        "latest_rank",
    )
    .orderBy(F.col("brand").desc())
)
