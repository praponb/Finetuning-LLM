from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy("last_restock_period", "id")
w2 = Window.partitionBy("product_id").orderBy("last_restock_period", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("product_id").orderBy(F.col("burst_id").desc(), "id")
w_grp = Window.partitionBy("product_id").orderBy("next_quantity", "id").rowsBetween(-3, Window.currentRow)

base = (
    df_inventory
    .filter(F.col("last_restock") >= "2024-05-26")
    .select(
        "id",
        "product_id",
        "quantity",
        "last_restock",
        F.date_trunc("YEAR", "last_restock").alias("last_restock_period"),
    )
)

flagged = (
    base
    .withColumn("new_group_flag", F.when(F.col("last_restock_period") - F.lag("last_restock_period").over(w) > F.expr("INTERVAL 90 DAYS"), 1).otherwise(0))
)

sessionized = (
    flagged
    .select(
        "id",
        "product_id",
        "quantity",
        "last_restock",
        "last_restock_period",
        F.sum("new_group_flag").over(w2).alias("burst_id"),
    )
)

scored = (
    sessionized
    .select(
        "id",
        "product_id",
        "quantity",
        "burst_id",
        F.lead("burst_id").over(w).alias("next_burst_id"),
        F.lead("quantity").over(w).alias("next_quantity"),
        F.lag("quantity").over(w).alias("prev_quantity"),
        F.row_number().over(w).alias("row_seq"),
    )
)

latest = (
    scored
    .withColumn("rank_in_group", F.row_number().over(w3))
    .filter(F.col("rank_in_group") == 1)
    .drop("rank_in_group")
)

with_windows = (
    latest
    .select(
        "id",
        "product_id",
        "quantity",
        "burst_id",
        "next_burst_id",
        "next_quantity",
        "prev_quantity",
        "row_seq",
        F.avg("prev_quantity").over(w_grp).alias("last4_avg_prev_quantity"),
        F.max("prev_quantity").over(w_grp).alias("last4_max_prev_quantity"),
        F.sum("prev_quantity").over(w_grp).alias("last4_sum_prev_quantity"),
    )
)

result = (
    with_windows
    .groupBy(
        "product_id",
        "burst_id",
        F.when(F.col("quantity") >= 212, "HIGH").otherwise("LOW").alias("quantity_tier"),
    )
    .agg(
        F.round(F.avg("last4_sum_prev_quantity"), 2).alias("avg_last4_sum_prev_quantity"),
        F.round(F.avg("last4_avg_prev_quantity"), 2).alias("avg_last4_avg_prev_quantity"),
        F.count("*").alias("num_rows"),
        F.sum("row_seq").alias("total_row_seq"),
    )
    .orderBy(F.col("num_rows").desc_nulls_last())
)
