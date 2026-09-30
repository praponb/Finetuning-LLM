from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id").orderBy(F.col("last_restock_period").desc(), "id")
w_grp = Window.orderBy("last_restock", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy(F.month("last_restock"))
w4 = Window.orderBy("seq_no_share", "id").rowsBetween(-5, -1)
w5 = Window.orderBy("seq_no", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

valid = (
    df_inventory
    .filter((F.col("last_restock") > F.to_date(F.lit("2023-03-17"))) & (F.col("last_restock") > "2023-09-01"))
    .select(
        "id",
        "store_id",
        "product_id",
        "last_restock",
        F.col("quantity").cast("string").alias("quantity_num"),
        F.date_trunc("year", "last_restock").alias("last_restock_period"),
    )
)

with_windows = (
    valid
    .filter(F.col("store_id").isin([1, 2, 3, 5]))
    .select(
        "id",
        "last_restock",
        "quantity_num",
        "last_restock_period",
        F.row_number().over(w).alias("seq_no"),
    )
)

enriched = (
    with_windows
    .withColumn("running_avg_seq_no", F.avg("seq_no").over(w_grp))
    .withColumn("running_sum_seq_no", F.sum("seq_no").over(w_grp))
    .withColumn("running_count_seq_no", F.count("seq_no").over(w_grp))
    .withColumn("seq_no_share", F.round((F.col("seq_no") / F.sum("seq_no").over(w3)), 4))
)

metrics = (
    enriched
    .select(
        "id",
        "last_restock",
        "quantity_num",
        "seq_no",
        "running_count_seq_no",
        F.avg("running_count_seq_no").over(w4).alias("running_count_seq_no_avg_5_prior"),
        F.last("running_avg_seq_no", ignorenulls=True).over(w5).alias("last_known_running_avg_seq_no"),
    )
)

filtered = (
    metrics
    .filter(F.col("seq_no").between(5, 6))
    .select(
        "id",
        "last_restock",
        "quantity_num",
        "seq_no",
        "running_count_seq_no",
        "running_count_seq_no_avg_5_prior",
        F.col("running_count_seq_no_avg_5_prior").cast("double").alias("running_count_seq_no_avg_5_prior_num"),
    )
)

filtered_inventory = (
    filtered
    .filter(F.col("last_restock") >= F.lit("2023-08-13").cast("date"))
    .select(
        "id",
        "last_restock",
        "quantity_num",
        "seq_no",
        "running_count_seq_no",
        "running_count_seq_no_avg_5_prior",
        "running_count_seq_no_avg_5_prior_num",
        F.date_format("last_restock", "yyyy-MM").alias("last_restock_label"),
        F.datediff(F.current_date(), "last_restock").alias("days_since_last_restock"),
    )
)

result = (
    filtered_inventory
    .select(
        "id",
        "last_restock",
        "quantity_num",
        "running_count_seq_no_avg_5_prior_num",
        "last_restock_label",
        "days_since_last_restock",
    )
    .orderBy(F.col("id").desc())
)
