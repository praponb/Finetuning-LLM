from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.date_trunc("month", "last_restock"), "product_id").orderBy("quantity", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("product_id").orderBy("years_since_last_restock", "id")
w_hist = Window.orderBy("next_years_since_last_restock", "id")
w4 = Window.orderBy("next_years_since_last_restock_2", "id")
w5 = Window.orderBy("last_restock", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.orderBy("prev_running_years_since_last_restock", "id")
w7 = Window.orderBy("row_seq", "id").rowsBetween(-5, -1)
w8 = Window.orderBy("row_seq", "id")

cleaned = (
    df_inventory
    .filter(F.col("store_id").isin([1, 3, 4, 5]))
    .select(
        "id",
        "product_id",
        "quantity",
        "last_restock",
        (F.datediff(F.current_date(), "last_restock") / 365.25).alias("years_since_last_restock"),
    )
)

scored = (
    cleaned
    .filter(F.col("product_id").isin([2, 5, 8, 10]))
    .select(
        "id",
        "last_restock",
        "years_since_last_restock",
        F.sum("years_since_last_restock").over(w).alias("running_years_since_last_restock"),
        F.lead("years_since_last_restock", 1, 0).over(w2).alias("next_years_since_last_restock"),
    )
)

metrics = (
    scored
    .withColumn("prev_years_since_last_restock", F.lag("years_since_last_restock").over(w_hist))
    .withColumn("next_years_since_last_restock_2", F.lead("years_since_last_restock").over(w_hist))
    .withColumn("prev_running_years_since_last_restock", F.lag("running_years_since_last_restock").over(w_hist))
    .withColumn("next_running_years_since_last_restock", F.lead("running_years_since_last_restock").over(w_hist))
    .withColumn("row_seq", F.row_number().over(w_hist))
)

scored_inventory = (
    metrics
    .select(
        "id",
        "running_years_since_last_restock",
        "prev_years_since_last_restock",
        "next_years_since_last_restock_2",
        "prev_running_years_since_last_restock",
        "next_running_years_since_last_restock",
        "row_seq",
        F.ntile(5).over(w4).alias("next_years_since_last_restock_2_bucket"),
        F.first("next_running_years_since_last_restock", ignorenulls=True).over(w5).alias("first_known_next_running_years_since_last_restock"),
        F.lead("running_years_since_last_restock", 1).over(w6).alias("next_running_years_since_last_restock_2"),
        F.avg("next_years_since_last_restock_2").over(w7).alias("next_years_since_last_restock_2_avg_5_prior"),
        F.row_number().over(w8).alias("row_seq_2"),
    )
)

base = (
    scored_inventory
    .filter(F.col("row_seq_2").between(6, 7))
    .withColumn("row_seq_2_filled", F.coalesce("row_seq_2", F.lit(0)))
)

base_inventory = (
    base
    .filter(F.col("next_years_since_last_restock_2").between(63, 68))
    .select(
        "*",
        (F.col("next_running_years_since_last_restock_2") * F.col("next_running_years_since_last_restock")).alias("next_running_years_since_last_restock_2_times_next_running_years_since_last_restock"),
    )
)

result = (
    base_inventory
    .select(
        "id",
        "running_years_since_last_restock",
        "prev_years_since_last_restock",
        "next_years_since_last_restock_2",
        "next_running_years_since_last_restock",
        "row_seq",
        "next_years_since_last_restock_2_bucket",
        "next_running_years_since_last_restock_2",
        "row_seq_2",
    )
)
