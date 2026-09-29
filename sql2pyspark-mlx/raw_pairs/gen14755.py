from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_part = Window.orderBy("opened_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy(F.month("opened_date")).orderBy("sqft", "id")
w3 = Window.orderBy("running_sum_sqft")
w_recent = Window.orderBy("sqft", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.revenue").desc(), F.col("c.id").desc())
w_ordered = Window.orderBy("sqft", "id")
w7 = Window.orderBy(F.col("sqft").desc())
w8 = Window.orderBy("sqft", "id").rowsBetween(-6, Window.currentRow)

base = (
    df_stores
    .filter(F.col("city") == "Tokyo")
    .select(
        "id",
        "store_name",
        "opened_date",
        "sqft",
        F.coalesce("region", F.lit("Unknown")).alias("region_filled"),
        F.coalesce("region", F.lit("N/A")).alias("region_filled_2"),
    )
)

scored = (
    base
    .select(
        "id",
        "sqft",
        F.sum("sqft").over(w_part).alias("running_sum_sqft"),
        F.avg("sqft").over(w_part).alias("running_avg_sqft"),
        F.count("sqft").over(w_part).alias("running_count_sqft"),
        F.row_number().over(w2).alias("row_seq"),
    )
)

scored_stores = (
    scored
    .select(
        "*",
        F.dense_rank().over(w3).alias("running_sum_sqft_dense_rank"),
        F.last("sqft", ignorenulls=True).over(w_recent).alias("last_known_sqft"),
    )
)

latest_match = (
    scored_stores
    .alias("s")
    .join(df_store_sales.alias("c"), (F.col("c.store_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w5))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.revenue"))
)

kept = (
    latest_match
    .alias("s")
    .join(df_store_sales.alias("c"), (F.col("c.store_id") == F.col("s.id")) & (F.col("c.units").isNull()), "left_semi")
)

scored_stores_2 = (
    kept
    .filter(F.col("sqft").between(9077, 10106))
    .select(
        "id",
        "sqft",
        "running_avg_sqft",
        "row_seq",
        "revenue",
        F.lead("sqft").over(w_ordered).alias("next_sqft"),
        F.lead("running_sum_sqft_dense_rank").over(w_ordered).alias("next_running_sum_sqft_dense_rank"),
        F.row_number().over(w_ordered).alias("seq_no"),
    )
)

with_windows = (
    scored_stores_2
    .select(
        "id",
        "sqft",
        "running_avg_sqft",
        "row_seq",
        "next_running_sum_sqft_dense_rank",
        "seq_no",
        F.dense_rank().over(w7).alias("sqft_dense_rank"),
        F.avg("next_sqft").over(w8).alias("next_sqft_avg_6"),
    )
)

result = (
    with_windows
    .select(
        "id",
        "sqft",
        "running_avg_sqft",
        "row_seq",
        "next_running_sum_sqft_dense_rank",
        "seq_no",
        "sqft_dense_rank",
        "next_sqft_avg_6",
    )
    .orderBy(F.col("seq_no").desc())
)
