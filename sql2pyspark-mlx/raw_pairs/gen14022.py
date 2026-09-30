from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id").orderBy(F.col("opened_date").asc_nulls_first(), "id")
w_recent = Window.partitionBy("region").orderBy(F.col("opened_date").desc(), "id")
w_grp = Window.partitionBy("region", "store_id").orderBy(F.col("years_since_last_restock").asc_nulls_last())
w_running = Window.partitionBy("store_id", "region").orderBy("opened_date", "id")
w_recent_2 = Window.partitionBy(F.year("opened_date"), "region").orderBy(F.col("years_since_last_restock").desc_nulls_first())
w6 = Window.partitionBy(F.month("opened_date"), "store_id")
w7 = Window.partitionBy("region", "store_id").orderBy(F.col("prev_years_since_last_restock").asc_nulls_last(), "id")
w8 = Window.partitionBy("store_id").orderBy("opened_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w9 = Window.partitionBy("store_id").orderBy(F.col("years_since_last_restock_dense_rank").desc(), "id")
w10 = Window.partitionBy("store_id").orderBy("years_since_last_restock", "id").rowsBetween(-5, Window.currentRow)

valid = (
    df_inventory
    .filter(F.col("last_restock") > F.to_date(F.lit("2023-02-14")))
    .select(
        "id",
        "store_id",
        "quantity",
        "last_restock",
        (F.datediff(F.current_date(), "last_restock") / 365).alias("years_since_last_restock"),
    )
)

enriched = (
    valid
    .alias("src")
    .join(df_stores.alias("d"), F.col("src.store_id") == F.col("d.id"), "left")
    .select(
        "src.id",
        "src.store_id",
        "src.years_since_last_restock",
        F.col("d.region"),
        F.col("d.opened_date"),
    )
)

deduped = (
    enriched
    .withColumn("rank_in_group", F.row_number().over(w))
    .filter(F.col("rank_in_group") == 1)
    .drop("rank_in_group")
)

scored = (
    deduped
    .filter(F.col("store_id") == 2)
    .select(
        "*",
        F.row_number().over(w_recent).alias("row_seq"),
        F.cume_dist().over(w_grp).alias("years_since_last_restock_cume_dist"),
        F.lag("years_since_last_restock", 1).over(w_running).alias("prev_years_since_last_restock"),
        F.dense_rank().over(w_recent_2).alias("years_since_last_restock_dense_rank"),
        F.count("years_since_last_restock").over(w6).alias("opened_date_month_count_years_since_last_restock"),
    )
)

deduped_inventory = (
    scored
    .withColumn("row_num", F.row_number().over(w7))
    .filter(F.col("row_num") == 1)
    .select(
        "id",
        "store_id",
        "years_since_last_restock",
        "opened_date",
        "prev_years_since_last_restock",
        "years_since_last_restock_dense_rank",
        "opened_date_month_count_years_since_last_restock",
    )
)

scored_inventory = (
    deduped_inventory
    .withColumn("cumulative_prev_years_since_last_restock", F.sum("prev_years_since_last_restock").over(w8))
    .withColumn("seq_no", F.row_number().over(w9))
    .withColumn("years_since_last_restock_avg_5", F.avg("years_since_last_restock").over(w10))
)

result = (
    scored_inventory
    .select(
        "id",
        "years_since_last_restock",
        "opened_date",
        "prev_years_since_last_restock",
        "years_since_last_restock_dense_rank",
        "cumulative_prev_years_since_last_restock",
    )
)
