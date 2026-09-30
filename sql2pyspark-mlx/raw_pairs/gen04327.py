from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.year("last_restock")).orderBy("quantity_str")
w2 = Window.orderBy(F.col("quantity_str").desc_nulls_first())
w3 = Window.orderBy("quantity_adjusted", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.orderBy("quantity_str_filled")
w5 = Window.orderBy("quantity_adjusted", "id")
w6 = Window.orderBy("quantity_str", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w7 = Window.orderBy("quantity_str_rank", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w8 = Window.partitionBy(F.year("last_restock_period")).orderBy("first_known_quantity_adjusted", "id")
w9 = Window.orderBy(F.col("first_known_quantity_adjusted").asc_nulls_last())
w10 = Window.orderBy("event_seq", "id")

cleaned = (
    df_inventory
    .filter(F.col("last_restock") > F.lit("2023-04-25").cast("date"))
    .select(
        "id",
        "last_restock",
        F.col("quantity").cast("double").alias("quantity_str"),
        F.round((F.col("quantity") * 0.9), 2).alias("quantity_adjusted"),
    )
)

scored = (
    cleaned
    .filter(F.col("quantity_adjusted") < 41)
    .withColumn("quantity_str_percent_rank", F.percent_rank().over(w))
    .withColumn("quantity_str_rank", F.rank().over(w2))
)

base = (
    scored
    .filter(F.col("last_restock") >= F.lit("2023-03-31").cast("date"))
    .select(
        "id",
        "last_restock",
        "quantity_str",
        "quantity_adjusted",
        "quantity_str_percent_rank",
        "quantity_str_rank",
        F.date_trunc("MONTH", "last_restock").alias("last_restock_period"),
        F.coalesce("quantity_str", F.lit(0)).alias("quantity_str_filled"),
    )
)

scored_inventory = (
    base
    .select(
        "id",
        "quantity_str",
        "quantity_str_percent_rank",
        "last_restock_period",
        "quantity_str_filled",
        F.sum("quantity_str_rank").over(w3).alias("running_quantity_str_rank"),
        F.dense_rank().over(w4).alias("quantity_str_filled_dense_rank"),
        F.row_number().over(w5).alias("event_seq"),
        F.sum("quantity_adjusted").over(w6).alias("running_quantity_adjusted"),
        F.first("quantity_adjusted", ignorenulls=True).over(w7).alias("first_known_quantity_adjusted"),
    )
)

enriched = (
    scored_inventory
    .withColumn("row_seq", F.row_number().over(w8))
    .withColumn("first_known_quantity_adjusted_dense_rank", F.dense_rank().over(w9))
    .withColumn("event_seq_2", F.row_number().over(w10))
)

valid = (
    enriched
    .filter(F.col("last_restock_period") < "2024-06-01")
    .select(
        "id",
        "quantity_str",
        "quantity_str_percent_rank",
        "last_restock_period",
        "quantity_str_filled",
        "quantity_str_filled_dense_rank",
        "running_quantity_adjusted",
        "row_seq",
        "first_known_quantity_adjusted_dense_rank",
        "event_seq_2",
        (F.col("event_seq_2") + F.col("quantity_str_percent_rank")).alias("event_seq_2_plus_quantity_str_percent_rank"),
    )
)

result = (
    valid
    .select(
        "id",
        "quantity_str",
        "quantity_str_percent_rank",
        "quantity_str_filled",
        "running_quantity_adjusted",
        "row_seq",
        "event_seq_2_plus_quantity_str_percent_rank",
    )
)
