from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("unit_price_whole").desc(), "id")
w2 = Window.orderBy("unit_price_whole", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.orderBy("seq_no_minus_unit_price_whole_filled", "id").rowsBetween(-5, -1)
w4 = Window.orderBy("unit_price_whole_filled", "id").rowsBetween(-3, -1)
w_recent = Window.orderBy("seq_no", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.orderBy("unit_price_whole_avg_5_prior", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w7 = Window.orderBy(F.col("unit_price_whole_avg_5_prior").desc_nulls_first())
w_ordered = Window.orderBy("seq_no_minus_unit_price_whole_filled", "id")

base = (
    df_order_items
    .filter(F.col("product_id").isNull())
    .select("id", F.floor("unit_price").alias("unit_price_whole"))
)

with_windows = (
    base
    .select(
        "id",
        "unit_price_whole",
        F.row_number().over(w).alias("seq_no"),
        F.sum("unit_price_whole").over(w2).alias("running_unit_price_whole"),
    )
)

base_order_items = (
    with_windows
    .filter(F.col("seq_no") < 5)
    .select(
        "*",
        F.coalesce("unit_price_whole", F.lit(0)).alias("unit_price_whole_filled"),
    )
)

valid = (
    base_order_items
    .filter(F.col("seq_no") > 5)
    .select(
        "*",
        (F.col("seq_no") - F.col("unit_price_whole_filled")).alias("seq_no_minus_unit_price_whole_filled"),
    )
)

metrics = (
    valid
    .filter(F.col("seq_no_minus_unit_price_whole_filled").between(46, 58))
    .select(
        "id",
        "seq_no",
        "running_unit_price_whole",
        "seq_no_minus_unit_price_whole_filled",
        F.avg("unit_price_whole").over(w3).alias("unit_price_whole_avg_5_prior"),
        F.avg("unit_price_whole_filled").over(w4).alias("unit_price_whole_filled_avg_3_prior"),
    )
)

metrics_order_items = (
    metrics
    .select(
        "*",
        F.last("seq_no_minus_unit_price_whole_filled", ignorenulls=True).over(w_recent).alias("last_known_seq_no_minus_unit_price_whole_filled"),
        F.first("unit_price_whole_avg_5_prior", ignorenulls=True).over(w6).alias("first_known_unit_price_whole_avg_5_prior"),
        F.rank().over(w7).alias("unit_price_whole_avg_5_prior_rank"),
        F.lag("unit_price_whole_avg_5_prior", 1).over(w_ordered).alias("prev_unit_price_whole_avg_5_prior"),
    )
)

cleaned = (
    metrics_order_items
    .filter(F.col("unit_price_whole_avg_5_prior") < 59)
    .select(
        "*",
        (F.col("last_known_seq_no_minus_unit_price_whole_filled") + F.col("unit_price_whole_avg_5_prior")).alias("last_known_seq_no_minus_unit_price_whole_filled_plus_unit_price_whole_avg_5_prior"),
    )
)

result = (
    cleaned
    .select(
        "id",
        "last_known_seq_no_minus_unit_price_whole_filled_plus_unit_price_whole_avg_5_prior",
        "last_known_seq_no_minus_unit_price_whole_filled",
    )
    .unpivot(
        ids=["id"],
        values=["last_known_seq_no_minus_unit_price_whole_filled_plus_unit_price_whole_avg_5_prior", "last_known_seq_no_minus_unit_price_whole_filled"],
        variableColumnName="metric",
        valueColumnName="reading",
    )
    .filter(F.col("reading").isNotNull())
)
