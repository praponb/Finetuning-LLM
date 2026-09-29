from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id", "product_id").orderBy(F.col("revenue").desc_nulls_last())
w2 = Window.partitionBy("store_id").orderBy("revenue_adjusted", "id")
w3 = Window.orderBy("units", "id")
w_hist = Window.orderBy("revenue", "id")

valid = (
    df_store_sales
    .filter(F.col("sale_date") > F.lit("2023-05-21").cast("date"))
    .withColumn("revenue_adjusted", F.round((F.col("revenue") * 1.1), 2))
)

deduped = (
    valid
    .withColumn("row_num", F.rank().over(w))
    .filter(F.col("row_num") == 1)
    .select("id", "store_id", "units", "revenue", "revenue_adjusted")
)

cleaned = (
    deduped
    .filter(F.col("revenue") <= 2700)
    .select(
        "id",
        "store_id",
        "units",
        "revenue",
        "revenue_adjusted",
        (F.col("revenue_adjusted") - F.col("units")).alias("revenue_adjusted_minus_units"),
    )
)

scored = (
    cleaned
    .select(
        "id",
        "revenue",
        F.lag("units").over(w2).alias("prev_units"),
        F.lag("revenue").over(w2).alias("prev_revenue"),
        F.lead("revenue").over(w2).alias("next_revenue"),
        F.row_number().over(w2).alias("seq_no"),
        F.lead("units", 1).over(w3).alias("next_units"),
    )
)

base = (
    scored
    .filter(F.col("next_units").isNotNull())
    .select(
        "id",
        "revenue",
        "prev_units",
        "prev_revenue",
        "next_revenue",
        F.coalesce("prev_revenue", F.lit(0)).alias("prev_revenue_filled"),
        F.col("next_units").cast("string").alias("next_units_num"),
    )
)

metrics = (
    base
    .filter(F.col("prev_revenue") <= 1800)
    .select(
        "*",
        F.lead("prev_units").over(w_hist).alias("next_prev_units"),
        F.lag("prev_units").over(w_hist).alias("prev_prev_units"),
        F.lag("prev_revenue").over(w_hist).alias("prev_prev_revenue"),
        F.lead("prev_revenue").over(w_hist).alias("next_prev_revenue"),
        F.row_number().over(w_hist).alias("event_seq"),
    )
)

result = (
    metrics
    .select(
        "id",
        "prev_units",
        "next_revenue",
        "prev_revenue_filled",
        "next_prev_units",
        "prev_prev_revenue",
        "next_prev_revenue",
        "event_seq",
    )
    .orderBy("prev_units", F.col("prev_revenue_filled").desc())
)
